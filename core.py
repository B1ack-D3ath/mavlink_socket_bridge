import argparse
import time
import sys
import logging
import os
import queue
import uuid
import importlib
import json
from typing import Dict, Optional, List, Any

# Proje modüllerini import et
from buffer_manager import BufferManager
from mavlink_handler.mavlink_handler_copter import MAVLinkHandlerCopter
from socketio_connection import SocketIOConnection

# --- Global Değişkenler ---
mav_copter: Optional[MAVLinkHandlerCopter] = None
socket_client: Optional[SocketIOConnection] = None
buffer: Optional[BufferManager] = None
logger: Optional[logging.Logger] = None
OPERATION_MAP: Dict[str, str] = {}


active_operations: Dict[str, Any] = {}
operation_output_queue = queue.Queue()

# -----------------------------

def setup_logging(log_level_str="INFO", log_file="mavlink_bridge.log"):
    """Dosya ve konsol için loglama ayarlarını yapar."""
    
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')
    log_dir = os.path.dirname(log_file)
    
    if log_dir and not os.path.exists(log_dir):
        try: os.makedirs(log_dir)
        except OSError as e:
            print(f"KRİTİK: Log dizini oluşturulamadı {log_dir}: {e}.", file=sys.stderr)
            log_file = os.path.basename(log_file)
    
    log_handler = logging.FileHandler(log_file, mode='a')
    log_handler.setFormatter(log_formatter)
    root_logger = logging.getLogger()
    
    if root_logger.hasHandlers(): root_logger.handlers.clear()
    
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
    
    parser.add_argument('--srv-ptc', default='http', help='WebSocket sunucu protokolü (http/https)')
    parser.add_argument('--srv-host', default='localhost', help='WebSocket sunucu adresi')
    parser.add_argument('--srv-port', default='3000', help='WebSocket sunucu portu')
    parser.add_argument('--srv-token', default='bridge', help='WebSocket sunucu tokeni (opsiyonel)')
    parser.add_argument('--mv-url', default='udp:localhost:14550', help='MAVLink bağlantı adresi')
    parser.add_argument('--mv-source-system', default=255, type=int, help='Bu köprünün MAVLink kaynak sistem IDsi')
    parser.add_argument('--buffer-size', default=50, type=int, help='Flush öncesi tampondaki maks. mesaj sayısı')
    parser.add_argument('--flush-timeout', default=1.0, type=float, help='Flush öncesi mesajlar arasındaki maks. süre (s)')
    parser.add_argument('--log-file', default='mavlink_bridge.log', help='Log dosyasının yolu')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Dosya için loglama seviyesi')
    parser.add_argument('--socket-check-interval', default=5.0, type=float, help='Socket.IO bağlantı kontrol aralığı (s)')
    parser.add_argument('--socket-max-disconnect', default=30.0, type=float, help='Çıkış öncesi maks. Socket.IO kopukluk süresi (s)')
    parser.add_argument('--loop-sleep', default=0.01, type=float, help='Ana döngü bekleme süresi (s)')
    parser.add_argument('--op-config', default='config/operations.json', help='Operasyon yapılandırma dosyasının yolu')
    
    return parser.parse_args()

def load_operations_config(filepath: str) -> Optional[Dict[str, str]]:
    """Operasyon yapılandırma dosyasını (JSON) okur ve bir sözlük döndürür."""
    try:
        with open(filepath, 'r') as f:
            config_data = json.load(f)
        logger.info(f"{len(config_data)} adet operasyon '{filepath}' dosyasından başarıyla yüklendi.")
        return config_data
    
    except FileNotFoundError:
        logger.error(f"Operasyon yapılandırma dosyası bulunamadı: {filepath}")
        return None
    
    except json.JSONDecodeError as e:
        logger.error(f"Operasyon yapılandırma dosyası hatalı (JSON formatında değil): {filepath}. Hata: {e}")
        return None
    
    except Exception as e:
        logger.error(f"Operasyon yapılandırma dosyası okunurken beklenmedik bir hata oluştu: {e}")
        return None

def get_operation_class(class_path: str) -> Optional[type]:
    """Verilen yolu kullanarak bir sınıfı dinamik olarak import eder."""
    try:
        module_path, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    
    except (ImportError, AttributeError) as e:
        logger.error(f"Operasyon sınıfı yüklenemedi: {class_path}. Hata: {e}")
        return None

def handle_start_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """Socket.IO'dan gelen operasyon başlatma isteğini dinamik olarak işler."""
    
    op_name = data.get("operation_name")
    params = data.get("params", {})
    op_id = data.get("id", "None")
    logger.info(f"'{op_name}' operasyonunu başlatma isteği alındı (ID: {op_id}).")
    
    class_path = OPERATION_MAP.get(op_name)
    if not class_path:
        logger.warning(f"Bilinmeyen operasyon isteği: {op_name}. Yapılandırma dosyasını kontrol edin.")
        return {'success': False, 'id': op_id, 'error': f'Unknown operation: {op_name}'}
    
    OperationClass = get_operation_class(class_path)
    if not OperationClass:
        return {'success': False, 'id': op_id, 'error': f'Operation class not found at {class_path}'}
    
    if not mav_copter or not mav_copter.is_ready():
        return {'success': False, 'id': op_id, 'error': 'MAVLink connection not available.'}
    
    try:
        operation_instance = OperationClass(mav_copter, op_id, operation_output_queue, params, logger)
        if operation_instance.start():
            active_operations[op_id] = operation_instance
            return {'success': True, 'id': op_id}
        
        else:
            return {'success': False, 'id': op_id, 'error': 'Operation failed to start.'}
    
    except Exception as e:
        return {'success': False, 'id': op_id, 'error': f'Internal bridge error: {e}'}

def handle_stop_operation(data: Dict[str, Any]) -> Dict[str, Any]:
    """Socket.IO'dan gelen operasyon durdurma isteğini işler."""
    
    op_id = data.get("id")
    operation_instance = active_operations.get(op_id)
    
    if not operation_instance:
        return {'success': False, 'id': op_id, 'error': 'Operation ID not found.'}
    
    try:
        operation_instance.stop()
        del active_operations[op_id]
        return {'success': True, 'id': op_id}
    
    except Exception as e:
        return {'success': False, 'id': op_id, 'error': f'Internal bridge error: {e}'}

def handle_mavlink_command(data: Dict[str, Any]) -> int:
    if mav_copter: return mav_copter.send_command(data)
    return False

def handle_mavlink_mission_download() -> Optional[List[Dict[str, Any]]]:
    if mav_copter: return mav_copter.mission_get()
    return None

def handle_mavlink_mission_upload(items: List[Dict[str, Any]]) -> bool:
    if mav_copter: return mav_copter.mission_set(items)
    return False

# --- Ana Program ---
def main():
    """Uygulamayı başlatır ve ana döngüyü çalıştırır."""
    global mav_copter, socket_client, buffer, logger, OPERATION_MAP
    
    args = parse_args()
    logger = setup_logging(log_level_str=args.log_level, log_file=args.log_file)
    logger.info("--- MAVLink Köprüsü Başlatılıyor ---")
    
    OPERATION_MAP = load_operations_config(args.op_config)
    if OPERATION_MAP is None:
        logger.critical("Operasyonlar yüklenemedi. Çıkılıyor.")
        sys.exit(1)
    
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
            if mav_copter and mav_copter.connection_failed_permanently: break
            if mav_copter:
                try:
                    msg = mav_copter.received_messages.get_nowait()
                    if msg and buffer.add_message(msg):
                        if socket_client: socket_client.flush_buffer(buffer)
                
                except queue.Empty: pass
            
            if buffer and buffer.check_timeout():
                if socket_client: socket_client.flush_buffer(buffer)
            
            try:
                status_op = operation_output_queue.get_nowait()
                if socket_client: socket_client.emit_status('status_operation', status_op)
            
            except queue.Empty: pass
            if not socket_client.check_persistent_disconnect(): break
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
