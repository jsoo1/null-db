final: prev: {
  null-db = (final.callPackage ../database { }).overrideAttrs (o: {
    # passthru is a nixpkgs field that is reserved to only exist in
    # nix runtime but not end up in a derivation. passthru.tests is
    # one very common attribute.
    passthru.tests.basic-raft = final.callPackage ./nixos-tests/basic-raft { };
  });
}
