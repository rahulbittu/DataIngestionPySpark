{pkgs}: {
  deps = [
    pkgs.rdkafka
    pkgs.glibcLocales
    pkgs.libyaml
    pkgs.postgresql
    pkgs.openssl
  ];
}
