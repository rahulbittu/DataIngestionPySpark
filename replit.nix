{pkgs}: {
  deps = [
    pkgs.libffi
    pkgs.cyrus_sasl
    pkgs.rdkafka
    pkgs.glibcLocales
    pkgs.libyaml
    pkgs.postgresql
    pkgs.openssl
  ];
}
