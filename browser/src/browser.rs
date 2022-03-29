use crate::{
    err_to_jsvalue, web_cache_fs::WebCacheDir, worker_scope, JsError, PagingFile, SimpleWebCacheFs,
};
use futures::FutureExt;
use js_sys::{Date, Promise, JSON};
use log::{info, trace};
use parking_lot::Mutex;
use radixdb::{BlobStore, DynBlobStore, TreeNode};
use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    ffi::CStr,
    fmt::{Debug, Display},
    io::{self, ErrorKind, Read, Seek, Write},
    ops::Range,
    sync::Arc,
};
use wasm_bindgen::JsValue;
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use wasm_futures_executor::ThreadPool;
use web_sys::DedicatedWorkerGlobalScope;

#[wasm_bindgen]
pub async fn start() -> Result<JsValue, JsValue> {
    let _ = console_log::init_with_level(log::Level::Info);
    ::console_error_panic_hook::set_once();
    pool_test().await.map_err(err_to_jsvalue)?;
    Ok("done".into())
}

async fn pool_test() -> anyhow::Result<()> {
    let pool = ThreadPool::new(2).await.map_err(JsError::from)?;
    let pool2 = pool.clone();
    // the outer spawn is necessary so we don't block on the main thread, which is not allowed
    pool.spawn_lazy(move || {
        async move {
            // info!("starting cache_bench");
            // cache_bench().await.unwrap();
            info!("starting sqlite_test");
            sqlite_test(pool2).unwrap();
            // info!("starting cache_test");
            // cache_test().await.unwrap();
            // info!("starting cache_test_sync");
            // cache_test_sync(pool2).unwrap();
        }
        .boxed_local()
    });
    Ok(())
}

async fn cache_test() -> anyhow::Result<()> {
    info!("{}", worker_scope().location().origin());
    let dir = WebCacheDir::new("test").await?;
    let deleted = dir.delete("file1").await?;
    info!("deleted {}", deleted);
    let mut file = dir.open_file("file1".into()).await?;
    for i in 0..5u8 {
        let mut data = vec![i; 100];
        file.append(&mut data).await?;
    }
    let data = file.read(10..240).await?;
    info!("{:?}", dir);
    info!("{:?}", file);
    info!("{}", hex::encode(data));
    Ok(())
}

async fn cache_bench() -> anyhow::Result<()> {
    let dir = WebCacheDir::new("bench").await?;
    let mut file = dir.open_file("file1".into()).await?;
    let mut data = vec![0u8; 1024 * 1024 * 4];
    data[0..4].copy_from_slice(&100u32.to_be_bytes());
    let t0 = js_sys::Date::now();
    file.append(&mut data).await?;
    let dt = js_sys::Date::now() - t0;
    info!("write {}", dt);
    let t0 = js_sys::Date::now();
    let blob = file.load_length_prefixed(0).await?;
    let dt = js_sys::Date::now() - t0;
    info!("read {} {}", dt, blob.len());
    Ok(())
}

fn now() -> f64 {
    Date::now() / 1000.0
}

fn do_test(mut store: DynBlobStore) -> anyhow::Result<()> {
    let elems = (0..2000000).map(|i| {
        if i % 100000 == 0 {
            info!("{}", i);
        }
        (
            i.to_string().as_bytes().to_vec(),
            i.to_string().as_bytes().to_vec(),
        )
    });
    let t0 = now();
    info!("building tree");
    let mut tree: TreeNode = elems.collect();
    info!("unattached tree {:?} {} s", tree, now() - t0);
    info!("traversing unattached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("attaching tree...");
    let t0 = now();
    tree.attach(&mut store)?;
    store.sync()?;
    info!("attached tree {:?} {} s", tree, now() - t0);
    info!("traversing attached tree values...");
    let t0 = now();
    let mut n = 0;
    for item in tree.try_values(&store) {
        if item.is_err() {
            info!("{:?}", item);
        }
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("traversing attached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    info!("detaching tree...");
    let t0 = now();
    tree.detach(&store, true)?;
    info!("detached tree {:?} {} s", tree, now() - t0);
    info!("traversing unattached tree...");
    let t0 = now();
    let mut n = 0;
    for _ in tree.try_iter(&store)? {
        n += 1;
    }
    info!("done {} items, {} s", n, now() - t0);
    Ok(())
}

fn cache_test_sync(pool: ThreadPool) -> anyhow::Result<()> {
    let fs = SimpleWebCacheFs::new(pool);
    fs.delete_dir("sync")?;
    let dir = fs.open_dir("sync")?;
    dir.delete_file("test")?;
    let file = dir.open_file("test")?;
    let file = PagingFile::new(file, 1024 * 1024)?;
    let store: DynBlobStore = Box::new(file);
    do_test(store)
}

fn sqlite_test(pool: ThreadPool) -> anyhow::Result<()> {
    use rusqlite::{Connection, OpenFlags};
    let fs = SimpleWebCacheFs::new(pool);
    fs.delete_dir("sqlite")?;
    let dir = fs.open_dir("sqlite")?;

    sqlite_vfs::register("test", dir).unwrap();
    let conn = Connection::open_with_flags_and_vfs(
        "db/main.db3",
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        "test",
    )?;

    conn.execute_batch(
        r#"
        PRAGMA page_size = 32768;
        PRAGMA journal_mode = MEMORY;
        "#,
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS vals (id INT PRIMARY KEY, val VARCHAR NOT NULL)",
        [],
    )?;

    for i in 0..10 {
        conn.execute("INSERT INTO vals (val) VALUES ('test')", [])?;
    }

    let n: i64 = conn.query_row("SELECT COUNT(*) FROM vals", [], |row| row.get(0))?;

    info!("Count: {}", n);
    conn.cache_flush()?;
    drop(conn);
    Ok(())
}
