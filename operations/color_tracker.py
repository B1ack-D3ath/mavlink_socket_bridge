# Bu dosya, projenizin ana dizininde 'operations' adlı klasör içinde yer almalıdır.
# operation_01.py dosyasındaki Raspberry Pi için optimize edilmiş mantığı kullanır.

import cv2
import threading
import numpy as np
import math
import time
import uuid
import logging
from queue import Queue
from typing import Dict, Any, Optional

# --- Hedef Yönetim Sınıfları (operation_01.py'den uyarlandı) ---

class Target:
    """Tek bir hedefi temsil eden sınıf."""
    def __init__(self, pixel_coords: tuple, gps_coords: tuple, confirmation_frames: int):
        self.id = uuid.uuid4()
        self.last_pixel_coords = pixel_coords
        self.gps_coords = gps_coords
        self.confirmation_counter = 1
        self.frames_unseen = 0
        self.is_reported = False
        self._confirmation_frames = confirmation_frames

    def update(self, pixel_coords: tuple, gps_coords: tuple):
        """Hedefin bilgilerini günceller."""
        self.last_pixel_coords = pixel_coords
        self.gps_coords = gps_coords
        self.frames_unseen = 0
        if self.confirmation_counter <= self._confirmation_frames:
            self.confirmation_counter += 1

class TargetManager:
    """Tespit edilen tüm hedefleri yöneten, güncelleyen ve raporlayan sınıf."""
    def __init__(self, output_queue: Queue, config: Dict[str, Any]):
        self.targets = []
        self.output_queue = output_queue
        self.config = config

    def find_closest_target(self, pixel_coords: tuple) -> Optional[Target]:
        """Verilen piksel koordinatlarına eşik mesafesi içindeki en yakın hedefi bulur."""
        closest_target = None
        min_dist = float('inf')
        for target in self.targets:
            dist = math.hypot(pixel_coords[0] - target.last_pixel_coords[0], 
                              pixel_coords[1] - target.last_pixel_coords[1])
            if dist < self.config['pixel_threshold'] and dist < min_dist:
                min_dist = dist
                closest_target = target
        return closest_target

    def update(self, new_detections: list, mav_telemetry: Dict[str, Any], frame_shape: tuple):
        """Yeni tespitlerle hedef listesini günceller ve raporlanması gerekenleri kuyruğa ekler."""
        updated_targets_in_frame = set()
        
        for pixel in new_detections:
            gps = calculate_target_gps(frame_shape, pixel, mav_telemetry, self.config)
            if not gps: 
                continue

            closest = self.find_closest_target(pixel)
            if closest:
                closest.update(pixel, gps)
                updated_targets_in_frame.add(closest.id)
            else:
                self.targets.append(Target(pixel, gps, self.config['confirmation_frames']))

        for target in self.targets:
            if target.id not in updated_targets_in_frame:
                target.frames_unseen += 1
            
            if not target.is_reported and target.confirmation_counter >= self.config['confirmation_frames']:
                lat, lon = target.gps_coords
                report = {
                    "type": "target_detected",
                    "operation_type": "color_tracker",
                    "id": str(target.id),
                    "lat": lat,
                    "lon": lon,
                    "timestamp": time.time()
                }
                self.output_queue.put(report)
                target.is_reported = True
        
        self.targets = [t for t in self.targets if t.frames_unseen < self.config['unseen_threshold']]


# --- Görüntü İşleme ve Hesaplama Fonksiyonları (operation_01.py'den uyarlandı) ---

def detect_all_color_targets(frame: np.ndarray, config: Dict[str, Any]) -> list:
    """
    Bir görüntüdeki hedefleri, Raspberry Pi için optimize edilmiş 
    "Hibrit Filtreleme" yöntemiyle bulur.
    """
    original_height, original_width = frame.shape[:2]
    if original_width == 0: return []
    
    scale_ratio = original_width / config['resize_width']
    new_height = int(original_height / scale_ratio)
    resized_image = cv2.resize(frame, (config['resize_width'], new_height), interpolation=cv2.INTER_LINEAR)

    hsv = cv2.cvtColor(resized_image, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, config['hsv_lower'], config['hsv_upper'])

    kernel = np.ones((3, 3), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    detected_centers = []
    if len(contours) > 0:
        sorted_contours = sorted(contours, key=cv2.contourArea, reverse=True)
        
        for contour in sorted_contours[:config['top_n_contours']]:
            if cv2.contourArea(contour) < config['min_contour_area']:
                break

            hull = cv2.convexHull(contour)
            if cv2.contourArea(hull) > 0:
                solidity = float(cv2.contourArea(contour)) / cv2.contourArea(hull)
                
                # YENİ: Solidity kontrolü artık yapılandırma dosyasından geliyor
                if solidity > config['solidity']:
                    x, y, w, h = cv2.boundingRect(contour)
                    
                    orig_cX = int((x + w / 2) * scale_ratio)
                    orig_cY = int((y + h / 2) * scale_ratio)
                    
                    detected_centers.append((orig_cX, orig_cY))
    
    return detected_centers

def calculate_target_gps(frame_shape: tuple, target_pixel: tuple, telemetry: Dict[str, Any], config: Dict[str, Any]) -> Optional[tuple]:
    """Hedefin GPS koordinatlarını, dron telemetrisi ve kamera açısından yola çıkarak hesaplar."""
    drone_alt = telemetry.get('alt', 0)
    if drone_alt <= 0.5: return None

    frame_height, frame_width = frame_shape[:2]
    dx = target_pixel[0] - frame_width / 2
    dy = frame_height / 2 - target_pixel[1]

    angle_offset_yaw = (dx / (frame_width / 2)) * (config['camera_fov_h'] / 2)
    angle_offset_pitch = (dy / (frame_height / 2)) * (config['camera_fov_v'] / 2)
    
    total_target_yaw_deg = telemetry.get('yaw', 0) + angle_offset_yaw
    total_camera_pitch = telemetry.get('pitch', 0) + telemetry.get('camera_fixed_pitch', 0)
    depression_angle_deg = -(total_camera_pitch + angle_offset_pitch)

    if depression_angle_deg <= 1.0: return None
    
    ground_distance = drone_alt / math.tan(math.radians(depression_angle_deg))
    
    R = 6378137.0
    dn = ground_distance * math.cos(math.radians(total_target_yaw_deg))
    de = ground_distance * math.sin(math.radians(total_target_yaw_deg))
    
    current_lat_rad = math.radians(telemetry.get('lat', 0))
    dLat = dn / R
    dLon = de / (R * math.cos(current_lat_rad))
    
    new_lat = telemetry.get('lat', 0) + math.degrees(dLat)
    new_lon = telemetry.get('lon', 0) + math.degrees(dLon)
    
    return (new_lat, new_lon)

# --- Ana Operasyon Sınıfı ---
class OperationColorTracker:
    def __init__(self, mav_handler, output_queue: Queue, params: Dict[str, Any], logger: logging.Logger):
        self.mav_handler = mav_handler
        self.output_queue = output_queue
        self.logger = logger
        self.is_running = False
        self.thread: Optional[threading.Thread] = None

        # Sunucudan gelen parametreleri ve operation_01.py'deki varsayılanları birleştir
        self.config = {
            'gstreamer_pipeline': params.get('gstreamer_pipeline', 0),
            'camera_fov_h': params.get('camera_fov_h', 58.5),
            'camera_fov_v': params.get('camera_fov_v', 58.5),
            'resize_width': params.get('resize_width', 320),
            'top_n_contours': params.get('top_n_contours', 10),
            'min_contour_area': params.get('min_contour_area', 25),
            'solidity': params.get('solidity', 0.85), # YENİ: Solidity parametresi eklendi
            'hsv_lower': np.array(params.get('hsv_lower_bound', [90, 50, 40])),
            'hsv_upper': np.array(params.get('hsv_upper_bound', [115, 255, 255])),
            'confirmation_frames': params.get('confirmation_frames', 20),
            'pixel_threshold': params.get('pixel_threshold', 100),
            'unseen_threshold': params.get('unseen_threshold', 50),
        }

        self.target_manager = TargetManager(self.output_queue, self.config)

    def start(self) -> bool:
        """Operasyonu ayrı bir iş parçacığında (thread) başlatır."""
        if self.is_running:
            self.logger.warning("Color Tracker (Optimized) operation is already running.")
            return False
        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info("Color Tracker (Optimized) operation started.")
        return True

    def stop(self):
        """Çalışan operasyon thread'ini durdurur."""
        if not self.is_running: return
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        self.logger.info("Color Tracker (Optimized) operation stopped.")

    def _run_loop(self):
        """Ana operasyon döngüsü. Görüntüyü alır, işler ve hedefleri bulur."""
        video_source = self.config['gstreamer_pipeline']
        
        if isinstance(video_source, str) and '!' in video_source:
             cap = cv2.VideoCapture(video_source, cv2.CAP_GSTREAMER)
        else:
             try: video_source = int(video_source)
             except (ValueError, TypeError): pass
             cap = cv2.VideoCapture(video_source)

        if not cap.isOpened():
            self.logger.error(f"KRİTİK: Görüntü kaynağı açılamadı! Kaynak: {video_source}")
            self.is_running = False
            return

        self.logger.info(f"Görüntü akışı başlatıldı: {video_source}")
        while self.is_running:
            ret, frame = cap.read()
            if not ret:
                time.sleep(0.1)
                continue
            
            mav_telemetry = self.mav_handler.get_telemetry_snapshot()
            mav_telemetry.update(self.config)

            detections = detect_all_color_targets(frame, self.config)
            self.target_manager.update(detections, mav_telemetry, frame.shape)
            
            time.sleep(0.02)

        cap.release()
        self.logger.info("Görüntü yakalama serbest bırakıldı.")
