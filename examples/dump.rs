use radixdb::{radixtree, RadixTree};

fn dump(indent: usize, tree: RadixTree) {
    println!(
        "{} {:?}=>{:?}",
        "  ".repeat(indent),
        tree.prefix(),
        tree.value()
    );
    let parent_prefix_len = tree.prefix().data().map(|x| x.len()).unwrap();
    // a tree is its own child, so we need to check is_leaf and prevent an empty prefix to prevent an endless loop
    if !tree.is_leaf() {
        for child in tree.group_by(|prefix, _| prefix.len() <= parent_prefix_len) {
            dump(indent + 1, child);
        }
    }
}

fn main() {
    let tree = radixtree! {
        "std::collections::BTreeMap" => "collection",
        "std::collections::BTreeSet" => "collection",
        "std::collections::HashMap" => "collection",
        "std::collections::HashSet" => "collection",
        "std::fmt::Debug" => "formatting",
        "std::fmt::Display" => "formatting",
    };
    dump(0, tree);
}
