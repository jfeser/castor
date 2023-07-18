{ lib, stdenv, fetchurl }:

stdenv.mkDerivation rec {
  pname = "cmph";
  version = "2.0.2";

  src = fetchurl {
    url =
      "https://sourceforge.net/projects/cmph/files/v2.0.2/cmph-2.0.2.tar.gz";
    sha256 = "sha256-Nl8egFZADUYPHue/r9vzfV7mx46PRyO/SzwIHIlzPx4=";
  };

  buildInputs = [ ];

  meta = with lib; {
    description = "cmph";
    homepage = "https://cmph.sourceforge.net/";
    license = licenses.lgpl2Only;
    maintainers = with maintainers; [ ];
  };
}
