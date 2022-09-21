use crate::{
    radixtree,
    store::{blob_store::OwnedBlob, BlobStore},
    RadixTree,
};
use hex_literal::hex;
use parking_lot::Mutex;
use std::{io::Write, sync::Arc};

#[derive(Debug, Clone)]
pub struct VecStore(Arc<Mutex<Vec<u8>>>);

impl BlobStore for VecStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<OwnedBlob, Self::Error> {
        let mut offset = usize::try_from(u64::from_be_bytes(id.try_into()?))?;
        let r = self.0.lock();
        anyhow::ensure!(offset >= 4 && offset <= r.len());
        let len = usize::try_from(u32::from_be_bytes(r[offset - 4..offset].try_into()?))?;
        offset -= 4;
        anyhow::ensure!(offset >= len);
        let res = r[offset - len..offset].to_vec();
        Ok(OwnedBlob::from_arc_vec(Arc::new(res)))
    }

    fn write(&self, data: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        let mut w = self.0.lock();
        // first data
        w.write_all(data)?;
        // then 4 byte len
        w.write_all(u32::try_from(data.len())?.to_be_bytes().as_ref())?;
        // id is end
        let id = u64::try_from(w.len())?;
        Ok(id.to_be_bytes().to_vec())
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

fn get_bytes(tree: RadixTree) -> Vec<u8> {
    let store = VecStore(Arc::new(Mutex::new(Vec::new())));
    let mut tree = tree.try_attached(store.clone()).unwrap();
    let v = store.0.lock();
    let v: &[u8] = v.as_ref();
    println!("{}", hex::encode(v));
    tree.try_reattach().unwrap()
}

#[test]
fn serde_tests() {
    assert_eq!(get_bytes(radixtree! { "" => "" }), hex!("000080"));
    assert_eq!(
        get_bytes(radixtree! { "hello" => "world!" }),
        hex!("0568656c6c6f06776f726c642180")
    );
    assert_eq!(
        get_bytes(radixtree! { [b'a'; 256] => [b'b'; 256] }),
        hex!("8961000000000000010488000000000000020880")
    );
    assert_eq!(
        get_bytes(radixtree! { "aa" => "", "ab" => "" }),
        hex!("0161808904000000000000000c")
    );
}
