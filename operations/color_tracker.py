# Bu dosya, projenizin ana dizininde 'operations' adlı yeni bir klasör içinde yer almalıdır.

import cv2
import threading
import numpy as np
import math
import time
import uuid
from queue import Queue
import logging
from typing import Dict, Any

# --- Hedef Yönetim Sistemi ---
# operation_01.py'den alınan Target ve TargetManager sınıfları,
# dışarıdan kontrol edilebilir ve modüler bir yapıya uyarlandı.

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
    def __init__(self, output_queue: Queue, confirmation_frames: int, pixel_threshold: int, unseen_threshold: int):
        self.targets = []
        self.output_queue = output_queue  # Sonuçları ana programa (core.py) göndermek için
        self.confirmation_frames = confirmation_frames
        self.pixel_distance_threshold = pixel_threshold
        self.unseen_frames_threshold = unseen_threshold

    def find_closest_target(self, pixel_coords: tuple) -> Target | None:
        """Verilen piksel koordinatlarına eşik mesafesi içindeki en yakın hedefi bulur."""
        closest_target = None
        min_dist = float('inf')
        for target in self.targets:
            dist = math.hypot(pixel_coords[0] - target.last_pixel_coords[0], 
                              pixel_coords[1] - target.last_pixel_coords[1])
            if dist < self.pixel_distance_threshold and dist < min_dist:
                min_dist = dist
                closest_target = target
        return closest_target

    def update(self, new_detections: list, mav_telemetry: Dict[str, Any], frame_shape: tuple):
        """Yeni tespitlerle hedef listesini günceller ve raporlanması gerekenleri kuyruğa ekler."""
        updated_targets_in_frame = set()
        
        for pixel in new_detections:
            gps = calculate_target_gps(frame_shape, pixel, mav_telemetry)
            if not gps: 
                continue

            closest = self.find_closest_target(pixel)
            if closest:
                closest.update(pixel, gps)
                updated_targets_in_frame.add(closest.id)
            else:
                self.targets.append(Target(pixel, gps, self.confirmation_frames))

        for target in self.targets:
            if target.id not in updated_targets_in_frame:
                target.frames_unseen += 1
            
            # Hedef yeterince görüldüyse ve henüz raporlanmadıysa, sonucu kuyruğa ekle
            if not target.is_reported and target.confirmation_counter >= self.confirmation_frames:
                lat, lon = target.gps_coords
                result_data = {
                    "type": "target_detected",
                    "operation_type": "color_tracker",
                    "id": str(target.id),
                    "lat": lat,
                    "lon": lon,
                    "timestamp": time.time()
                }
                self.output_queue.put(result_data)
                target.is_reported = True
        
        # Uzun süre görülmeyen hedefleri listeden temizle
        self.targets = [t for t in self.targets if t.frames_unseen < self.unseen_frames_threshold]


# --- Görüntü İşleme ve Hesaplama Fonksiyonları ---

def detect_all_color_targets(frame: np.ndarray, hsv_lower: np.ndarray, hsv_upper: np.ndarray, min_contour_area: int) -> list:
    """Görüntüdeki belirtilen renkteki tüm hedefleri bulur ve merkezlerinin piksel koordinatlarını döndürür."""
    hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
    mask = cv2.inRange(hsv, hsv_lower, hsv_upper)
    mask = cv2.erode(mask, None, iterations=2)
    mask = cv2.dilate(mask, None, iterations=2)
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    detected_centers = []
    for cnt in contours:
        if cv2.contourArea(cnt) > min_contour_area:
            M = cv2.moments(cnt)
            if M["m00"] != 0:
                cX = int(M["m10"] / M["m00"])
                cY = int(M["m01"] / M["m00"])
                detected_centers.append((cX, cY))
    return detected_centers

def calculate_target_gps(frame_shape: tuple, target_pixel: tuple, telemetry: Dict[str, Any]) -> tuple | None:
    """Hedefin GPS koordinatlarını, dron telemetrisi ve kamera açısından yola çıkarak hesaplar."""
    drone_alt = telemetry.get('alt', 0)
    if drone_alt <= 0.5: # Yere çok yakınken hesaplama yapma
        return None

    frame_height, frame_width = frame_shape[:2]
    dx = target_pixel[0] - frame_width / 2
    dy = frame_height / 2 - target_pixel[1]

    # Kamera FOV değerlerini telemetriden al
    camera_fov_h = telemetry.get('camera_fov_h', 60.0)
    camera_fov_v = telemetry.get('camera_fov_v', 45.0)

    angle_offset_yaw = (dx / (frame_width / 2)) * (camera_fov_h / 2)
    angle_offset_pitch = (dy / (frame_height / 2)) * (camera_fov_v / 2)
    
    total_target_yaw_deg = telemetry.get('yaw', 0) + angle_offset_yaw
    
    # Kameranın toplam eğim açısı (pitch), dronun kendi eğimi ve kameranın gövdeye göre sabit eğiminin toplamıdır.
    total_camera_pitch = telemetry.get('pitch', 0) + telemetry.get('camera_fixed_pitch', 0)
    depression_angle_deg = -(total_camera_pitch + angle_offset_pitch)

    if depression_angle_deg <= 1.0: # Yere paralel veya yukarı bakıyorsa hesaplama yapma
        return None
    
    ground_distance = drone_alt / math.tan(math.radians(depression_angle_deg))
    
    R = 6378137.0  # Dünya yarıçapı (metre)
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
        """
        Renk takibi operasyonunu yöneten sınıf.

        Args:
            mav_handler: Köprünün mevcut MAVLinkHandlerCopter nesnesi.
            output_queue: Sonuçların gönderileceği thread-safe kuyruk.
            params (dict): Sunucudan gelen, operasyona özel parametreler.
            logger: Kayıt tutmak için logger nesnesi.
        """
        self.mav_handler = mav_handler
        self.output_queue = output_queue
        self.params = params
        self.logger = logger
        self.is_running = False
        self.thread = None

        # Parametreleri al, yoksa varsayılan değerleri kullan
        self.pipeline = self.params.get("gstreamer_pipeline", "udpsrc port=5600 ! application/x-rtp,payload=96 ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! appsink")
        self.hsv_lower = np.array(self.params.get("hsv_lower_bound", [20, 100, 100]))
        self.hsv_upper = np.array(self.params.get("hsv_upper_bound", [30, 255, 255]))
        self.min_contour_area = self.params.get("min_contour_area", 250)
        
        self.target_manager = TargetManager(
            output_queue=self.output_queue,
            confirmation_frames=self.params.get("confirmation_frames", 10),
            pixel_threshold=self.params.get("pixel_distance_threshold", 100),
            unseen_threshold=self.params.get("unseen_frames_threshold", 50)
        )

    def start(self) -> bool:
        """Operasyonu ayrı bir iş parçacığında (thread) başlatır."""
        if self.is_running:
            self.logger.warning("Color Tracker operation is already running.")
            return False
        self.is_running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        self.logger.info("Color Tracker operation started.")
        return True

    def stop(self):
        """Çalışan operasyon thread'ini durdurur."""
        if not self.is_running:
            self.logger.warning("Attempted to stop an operation that is not running.")
            return
        self.is_running = False
        if self.thread:
            # Thread'in sonlanmasını bekle (timeout ile)
            self.thread.join(timeout=5.0)
            if self.thread.is_alive():
                self.logger.error("Color Tracker thread did not terminate gracefully.")
        self.logger.info("Color Tracker operation stopped.")

    def _run_loop(self):
        """Ana operasyon döngüsü. Görüntüyü alır, işler ve hedefleri bulur."""
        cap = cv2.VideoCapture(self.pipeline, cv2.CAP_GSTREAMER)
        if not cap.isOpened():
            self.logger.error(f"FATAL: Could not open video stream! Pipeline: {self.pipeline}")
            self.is_running = False
            return

        self.logger.info("Video stream capture started for color tracking operation.")
        while self.is_running:
            ret, frame = cap.read()
            if not ret:
                time.sleep(0.1) # Kameradan görüntü gelmiyorsa bekle
                continue
            
            # Gerekli telemetri verilerini MAVLink handler'dan anlık olarak al
            mav_telemetry = self.mav_handler.get_telemetry_snapshot()

            detections = detect_all_color_targets(frame, self.hsv_lower, self.hsv_upper, self.min_contour_area)
            self.target_manager.update(detections, mav_telemetry, frame.shape)
            
            # CPU kullanımını dengelemek için kısa bir bekleme
            time.sleep(0.02)

        cap.release()
        self.logger.info("Video capture released for color tracking operation.")
