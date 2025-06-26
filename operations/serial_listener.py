# Bu dosya, projenizin ana dizininde 'operations' adlı klasör içinde yer almalıdır.

import serial
import threading
import time
import logging
from queue import Queue
from typing import Dict, Any, Optional

class OperationSerialListener:
    """
    Listens to a specified serial port for incoming data, parses it,
    and puts it into a queue for the main application to process and send to the server.
    """
    def __init__(self,
                 mav_handler, # Not: Bu operasyon MAVLink'i kullanmaz, ancak standart yapı için parametre olarak alınır.
                 output_queue: Queue,
                 params: Dict[str, Any],
                 logger: logging.Logger):
        """
        Initializes the Serial Listener operation.

        Args:
            mav_handler: The MAVLink handler instance (not used in this operation).
            output_queue (Queue): The thread-safe queue to send results to.
            params (dict): Parameters for this operation, such as port and baudrate.
            logger (logging.Logger): The logger instance.
        """
        self.output_queue = output_queue
        self.params = params
        self.logger = logger
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.serial_connection: Optional[serial.Serial] = None

        # Parametreleri sunucudan gelen istekten al, yoksa varsayılan değerleri kullan
        self.port_name: str = self.params.get("port", "/dev/ttyUSB0") # Örnek port, sunucudan değiştirilebilir
        self.baud_rate: int = self.params.get("baudrate", 57600)

    def start(self) -> bool:
        """Opens the serial port and starts the listening thread."""
        if self.is_running:
            self.logger.warning("Serial Listener operation is already running.")
            return False

        try:
            # readline() metodunun sonsuza kadar beklemesini önlemek için timeout önemlidir.
            self.serial_connection = serial.Serial(self.port_name, self.baud_rate, timeout=1)
            self.logger.info(f"Seri port başarıyla açıldı: {self.port_name} @ {self.baud_rate} baud")
        except serial.SerialException as e:
            self.logger.error(f"KRİTİK: Seri port açılamadı {self.port_name}: {e}")
            return False

        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info("Serial Listener operasyonu başlatıldı.")
        return True

    def stop(self):
        """Stops the listening thread and closes the serial port."""
        if not self.is_running:
            return
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        if self.serial_connection and self.serial_connection.is_open:
            self.serial_connection.close()
            self.logger.info(f"Seri port kapatıldı: {self.port_name}")

    def _parse_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Gelen seri port satırını ayrıştırır.
        Beklenen format: "key1=value1,key2=value2,..."
        Örnek: "type=target,id=a1b2,lat=40.123,lon=29.456"
        """
        line = line.strip()
        if not line:
            return None

        try:
            data_dict = {}
            pairs = line.split(',')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split(':', 1)
                    data_dict[key.strip()] = value.strip()

            # Temel doğrulama: 'type' anahtarı zorunlu
            if "type" not in data_dict:
                self.logger.warning(f"Geçersiz seri veri alındı ('type' anahtarı eksik): {line}")
                return None

            # Bilinen alanlar için tip dönüşümü
            if 'lat' in data_dict: data_dict['lat'] = float(data_dict['lat'])
            if 'lon' in data_dict: data_dict['lon'] = float(data_dict['lon'])

            return data_dict

        except (ValueError, IndexError) as e:
            self.logger.error(f"Seri port satırı ayrıştırılamadı: '{line}'. Hata: {e}")
            return None

    def _run_loop(self):
        """Continuously reads from the serial port in a background thread."""
        self.logger.info(f"Seri porttan veri bekleniyor: {self.port_name}...")
        while self.is_running:
            if not self.serial_connection or not self.serial_connection.is_open:
                self.logger.error(f"Seri port bağlantısı koptu. Operasyon durduruluyor.")
                break

            try:
                # readline() metodu, yeni bir satır gelene veya timeout süresi dolana kadar bekler.
                line_bytes = self.serial_connection.readline()
                if line_bytes:
                    line_str = line_bytes.decode('utf-8').strip()
                    self.logger.debug(f"Seri porttan alındı: {line_str}")

                    parsed_data = self._parse_line(line_str)
                    if parsed_data:
                        # Veriyi standart bir formatta kuyruğa koy
                        report = {
                            "type": "external_data",      # Sunucunun bu veriyi tanıması için genel tip
                            "operation_type": "serial_listener", # Verinin hangi operasyondan geldiği
                            "timestamp": time.time(),
                            "payload": parsed_data        # Ayrıştırılmış asıl veri
                        }
                        self.output_queue.put(report)

            except serial.SerialException as e:
                self.logger.error(f"Seri port hatası: {e}. Operasyon durduruluyor.")
                break # Döngüden çıkarak thread'in sonlanmasını sağla
            except UnicodeDecodeError:
                self.logger.warning("Seri porttan UTF-8 olmayan veri alındı. Yoksayılıyor.")
            except Exception as e:
                self.logger.error(f"Seri port dinleyici döngüsünde beklenmedik hata: {e}", exc_info=True)
                time.sleep(1) # Hata tekrarını yavaşlat

        # Döngüden herhangi bir nedenle çıkılırsa, durumu güncelle
        self.is_running = False

