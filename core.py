import argparse
import time
import sys
import logging
import os
import queue
import uuid
from typing import Dict, Optional, List, Any

# Proje modüllerini import et
from buffer_manager import BufferManager
# Not: MAVLinkHandlerCopter sınıfının projenizde olduğunu ve
# gerekli metodları içerdiğini varsayıyoruz.
from mavlink_handler.mavlink_handler_copter import MAVLinkHandlerCopter
from socketio_connection import SocketIOConnection
from operations.color_tracker import OperationColorTracker

# --- Global Değişkenler ---
mav_copter: Optional[MAVLinkHandlerCopter] = None
socket_client: Optional[SocketIOConnection] = None
buffer: Optional[BufferManager] = None
logger: Optional[logging.Logger] = None

# Operasyon yönetimi için global değişkenler
active_operations: Dict[str, Any] = {}
operation_output_queue = queue.Queue()

# Sunucudan gelen operasyon ismine göre hangi sınıfın başlatılacağını belirler
OPERATION_MAP = {
    "color_tracker": OperationColorTracker
}
# -----------------------------

def setup_logging(log_level_str="INFO", log_file="mavlink_bridge.log"):
    """Dosya ve konsol için loglama ayarlarını yapar."""
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')
    
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Log dizini oluşturuldu: {log_dir}")
        except OSError as e:
            print(f"KRİTİK: Log dizini oluşturulamadı {log_dir}: {e}. Loglar mevcut dizine yazılacak.", file=sys.stderr)
            log_file = os.path.basename(log_file)

    log_handler = logging.FileHandler(log_file, mode='a')
    log_handler.setFormatter(log_formatter)

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.setLevel(log_level)
    root_logger.addHandler(log_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)

    logger_instance = logging.getLogger(__name__)
    logger_instance.info(f"Loglama ayarlandı. Seviye: {log_level_str}, Dosya: '{log_file}'")
    return logger_instance

def parse_args():
    """Komut satırı argümanlarını ayrıştırır."""
    parser = argparse.ArgumentParser(description='MAVLink to WebSocket Bridge')
    # Sunucu ayarları
    parser.add_argument('--srv-ptc', default='http', help='WebSocket sunucu protokolü (http/https)')
    parser.add_argument('--srv-host', default='localhost', help='WebSocket sunucu adresi')
    parser.add_argument('--srv-port', default='3000', help='WebSocket sunucu portu')
    parser.add_argument('--srv-token', default='bridge', help='WebSocket sunucu tokeni (opsiyonel)')
    # MAVLink ayarları
    parser.add_argument('--mv-url', default='udp:localhost:14550', help='MAVLink bağlantı adresi (örn: udp:ip:port, /dev/ttyUSB0:baud)')
    parser.add_argument('--mv-source-system', default=255, type=int, help='Bu köprünün MAVLink kaynak sistem IDsi')
    # Buffer ayarları
    parser.add_argument('--buffer-size', default=50, type=int, help='Flush öncesi tampondaki maksimum mesaj sayısı')
    parser.add_argument('--flush-timeout', default=1.0, type=float, help='Flush öncesi mesajlar arasındaki maksimum süre (saniye)')
    # Loglama ayarları
    parser.add_argument('--log-file', default='mavlink_bridge.log', help='Log dosyasının yolu')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Dosya için loglama seviyesi')
    # Yapılandırma ayarları
    parser.add_argument('--socket-check-interval', default=5.0, type=float, help='Socket.IO bağlantı kontrol aralığı (s)')
    parser.add_argument('--socket-max-disconnect', default=30.0, type=float, help='Çıkış öncesi maksimum Socket.IO bağlantı kopukluğu süresi (s)')
    parser.add_argument('--loop-sleep', default=0.01, type=float, help='Ana döngü bekleme süresi (s)')
    return parser.parse_args()

# --- Callback Fonksiyonları ---
def handle_mavlink_command(data: Dict[str, Any]) -> int:
    """MAVLink komut isteklerini işler."""
    if mav_copter: return mav_copter.send_command(data)
    logger.error("MAVLink bağlantısı yokken komut işlenemiyor.")
    return False

def handle_mavlink_mission_download() -> Optional[List[Dict[str, Any]]]:
    """MAVLink görev indirme isteklerini işler."""
    if mav_copter: return mav_copter.mission_get()
    logger.error("MAVLink bağlantısı yokken görev indirilemiyor.")
    return None

def handle_mavlink_mission_upload(items: List[Dict[str, Any]]) -> bool:
    """MAVLink görev yükleme isteklerini işler."""
    if mav_copter: return mav_copter.mission_set(items)
    logger.error("MAVLink bağlantısı yokken görev yüklenemiyor.")
    return False

def handle_start_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """Socket.IO'dan gelen operasyon başlatma isteğini işler ve durum döndürür."""
    op_name = data.get("operation_name")
    params = data.get("params", {})
    op_id = str(uuid.uuid4())
    logger.info(f"'{op_name}' operasyonunu başlatma isteği alındı (ID: {op_id}).")

    OperationClass = OPERATION_MAP.get(op_name)
    if not OperationClass:
        logger.warning(f"Bilinmeyen operasyon isteği: {op_name}")
        return {'success': False, 'id': op_id, 'error': f'Unknown operation: {op_name}'}

    if not mav_copter or not mav_copter.is_connected():
        logger.error(f"'{op_name}' başlatılamıyor: MAVLink bağlantısı yok.")
        return {'success': False, 'id': op_id, 'error': 'MAVLink connection not available.'}

    try:
        operation_instance = OperationClass(mav_copter, operation_output_queue, params, logger)
        if operation_instance.start():
            active_operations[op_id] = operation_instance
            logger.info(f"'{op_name}' operasyonu (ID: {op_id}) başarıyla başlatıldı.")
            return {'success': True, 'id': op_id}
        else:
            logger.error(f"'{op_name}' operasyonu (ID: {op_id}) başlatılamadı (start metodu False döndü).")
            return {'success': False, 'id': op_id, 'error': 'Operation failed to start.'}
    except Exception as e:
        logger.error(f"'{op_name}' operasyonu başlatılırken istisna oluştu: {e}", exc_info=True)
        return {'success': False, 'id': op_id, 'error': f'Internal bridge error: {e}'}

def handle_stop_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """Socket.IO'dan gelen operasyon durdurma isteğini işler ve durum döndürür."""
    op_id = data.get("operation_id")
    logger.info(f"Operasyon durdurma isteği alındı (ID: {op_id}).")

    operation_instance = active_operations.get(op_id)
    if not operation_instance:
        logger.warning(f"Durdurulmak istenen operasyon bulunamadı (ID: {op_id}).")
        return {'success': False, 'id': op_id, 'error': 'Operation ID not found.'}

    try:
        operation_instance.stop()
        del active_operations[op_id]
        logger.info(f"Operasyon (ID: {op_id}) başarıyla durduruldu ve listeden kaldırıldı.")
        return {'success': True, 'id': op_id}
    except Exception as e:
        logger.error(f"Operasyon (ID: {op_id}) durdurulurken istisna oluştu: {e}", exc_info=True)
        return {'success': False, 'id': op_id, 'error': f'Internal bridge error: {e}'}

# --- Ana Program ---
def main():
    global mav_copter, socket_client, buffer, logger

    args = parse_args()
    logger = setup_logging(log_level_str=args.log_level, log_file=args.log_file)
    logger.info("--- MAVLink Köprüsü Başlatılıyor ---")
    logger.debug(f"Argümanlar: {vars(args)}")
    
    SERVER_URL = f'{args.srv_ptc}://{args.srv_host}:{args.srv_port}?user={args.srv_token}'

    try:
        logger.info("MAVLink bağlantısı başlatılıyor...")
        mav_copter = MAVLinkHandlerCopter(args.mv_url, source_system=args.mv_source_system, logger=logger)

        logger.info("Socket.IO bağlantısı başlatılıyor...")
        socket_client = SocketIOConnection(
            server_url=SERVER_URL,
            handler_command=handle_mavlink_command,
            handler_mission_download=handle_mavlink_mission_download,
            handler_mission_upload=handle_mavlink_mission_upload,
            handler_start_operation=handle_start_operation,
            handler_stop_operation=handle_stop_operation,
            logger=logger,
            check_interval=args.socket_check_interval,
            max_disconnect_time=args.socket_max_disconnect
        )

        if not socket_client.connect():
            logger.critical("Socket.IO bağlantısı kurulamadı. Çıkılıyor.")
            if mav_copter: mav_copter.close()
            sys.exit(1)
        
        logger.info("Buffer Yöneticisi başlatılıyor...")
        buffer = BufferManager(buffer_size=args.buffer_size, flush_timeout=args.flush_timeout, logger=logger)

    except Exception as e:
        logger.critical(f"Başlatma sırasında kritik hata: {e}", exc_info=True)
        if socket_client: socket_client.disconnect()
        if mav_copter: mav_copter.close()
        sys.exit(1)

    logger.info("--- Ana uygulama döngüsü başlatıldı ---")
    while True:
        try:
            if mav_copter and mav_copter.connection_failed_permanently:
                logger.critical("MAVLink bağlantısı kalıcı olarak koptu. Çıkılıyor.")
                break

            if mav_copter:
                try:
                    msg = mav_copter.received_messages.get_nowait()
                    if msg and buffer.add_message(msg):
                        if socket_client: socket_client.flush_buffer(buffer)
                except queue.Empty:
                    pass

            if buffer and buffer.check_timeout():
                if socket_client: socket_client.flush_buffer(buffer)

            try:
                op_result = operation_output_queue.get_nowait()
                if socket_client:
                    socket_client.emit_status('operation_result', op_result)
            except queue.Empty:
                pass

            if not socket_client.check_persistent_disconnect():
                logger.critical("Socket.IO bağlantısı kalıcı olarak koptu. Çıkılıyor.")
                break
            
            time.sleep(args.loop_sleep)

        except KeyboardInterrupt:
            logger.info("Kullanıcı tarafından durduruldu (KeyboardInterrupt). Çıkılıyor...")
            break
        except Exception as e:
            logger.error(f"Ana döngüde beklenmedik hata: {e}", exc_info=True)
            time.sleep(1)

    logger.info("Kapanış prosedürü başlatılıyor...")
    for op_id, op_instance in list(active_operations.items()):
        logger.info(f"Çalışan operasyon durduruluyor: {op_id}")
        op_instance.stop()
    if socket_client: socket_client.disconnect()
    if mav_copter: mav_copter.close()
    logger.info("--- MAVLink Köprüsü Durduruldu ---")
    sys.exit(0)

if __name__ == "__main__":
    main()
