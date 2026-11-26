"""Servidor SMTP de prueba: acepta todo y guarda cada correo en un archivo.

Corre dentro del contenedor, con el Python de Airflow (solo biblioteca
estándar: ``smtpd`` ya no existe en Python 3.12)::

    python /checks/smtp_sink.py /work/mail 8025

Anuncia ``AUTH``, porque Airflow hace login cuando tiene usuario y contraseña,
y no ofrece STARTTLS (el escenario lo desactiva).
"""

import itertools
import socketserver
import sys
from pathlib import Path

OUT = Path(sys.argv[1])
PORT = int(sys.argv[2])
_counter = itertools.count(1)


class Handler(socketserver.StreamRequestHandler):
    def _send(self, line: str) -> None:
        self.wfile.write(f"{line}\r\n".encode())

    def handle(self) -> None:
        self._send("220 sink")
        data: list[bytes] | None = None
        while line := self.rfile.readline():
            if data is not None:
                if line.rstrip(b"\r\n") == b".":
                    path = OUT / f"{next(_counter):03d}.eml"
                    path.write_bytes(b"".join(data))
                    data = None
                    self._send("250 OK")
                else:
                    data.append(line[1:] if line.startswith(b"..") else line)
                continue
            command = line.decode(errors="replace").strip().upper()
            if command.startswith("EHLO"):
                self.wfile.write(b"250-sink\r\n250 AUTH PLAIN LOGIN\r\n")
            elif command.startswith("AUTH"):
                self._send("235 OK")
            elif command.startswith("DATA"):
                data = []
                self._send("354 go")
            elif command.startswith("QUIT"):
                self._send("221 bye")
                return
            else:  # HELO, MAIL, RCPT, RSET, NOOP
                self._send("250 OK")


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    with socketserver.TCPServer(("127.0.0.1", PORT), Handler) as server:
        server.serve_forever()
