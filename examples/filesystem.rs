use radixdb::{radixtree, RadixTree};

fn print_dirs(depth: usize, indent: usize, tree: RadixTree) {
    for child in tree.group_by(|key, node| key[depth..].iter().any(|x| *x == b'/')) {
        println!("{:?}", child);
    }
}

fn main() {
    let tree = radixtree! {
        "home/user1/file1" => "bar",
        "home/user2/file1" => "bar",
        "etc/passwd" => "bar",
    };
    print_dirs(0, 0, tree);
}
