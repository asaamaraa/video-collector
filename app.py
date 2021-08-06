import cv2
import time
from dotenv import load_dotenv
import os
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
import numpy as np

load_dotenv()

def gstreamer_pipeline(
    capture_width=1280,
    capture_height=720,
    display_width=1280,
    display_height=720,
    framerate=60,
    flip_method=0,
):
    return (
        "nvarguscamerasrc ! "
        "video/x-raw(memory:NVMM), "
        "width=(int)%d, height=(int)%d, "
        "format=(string)NV12, framerate=(fraction)%d/1 ! "
        "nvvidconv flip-method=%d ! "
        "video/x-raw, width=(int)%d, height=(int)%d, format=(string)BGRx ! "
        "videoconvert ! "
        "video/x-raw, format=(string)BGR ! appsink"
        % (
            capture_width,
            capture_height,
            framerate,
            flip_method,
            display_width,
            display_height,
        )
    )

class ImageProducer():
    def __init__(self, broker_url, topic):
        self.producer = KafkaProducer(bootstrap_servers=broker_url)
        self.topic = topic
        # self.camera = cv2.VideoCapture(0)  # use 0 for web camera
        self.camera = cv2.VideoCapture(gstreamer_pipeline(flip_method=2), cv2.CAP_GSTREAMER)
        time.sleep(0.1)
        self.process()

    def fps_info(self):
        # Number of frames to capture
        num_frames = 120
        print("Capturing {0} frames".format(num_frames))
    
        # Start time
        start = time.time()
    
        # Grab a few frames
        for i in range(0, num_frames) :
            ret, frame = self.camera.read()
    
        # End time
        end = time.time()
    
        # Time elapsed
        seconds = end - start
        print ("Time taken : {0} seconds".format(seconds))
    
        # Calculate frames per second
        fps  = num_frames / seconds
        print("Estimated frames per second : {0}".format(fps))

        return fps

    def send(self):
        try:
           
            future = self.producer.send(self.topic, self.buffer.tobytes())
            future.get(timeout=10)
            print('publish to %s frame size %s' % (self.topic, sys.getsizeof(self.buffer)))
        except KafkaError as e:
            print(e)
    
    def process(self):  #generate frame by frame from camera
        try:
            # self.fps_info()
            while self.camera.isOpened():
                # Capture frame-by-frame
                success, frame = self.camera.read()  # read the camera frame
                if not success:
                    print("Not success!")
                    break
                else:
                    ret, self.buffer = cv2.imencode('.jpg', frame)
                    self.send()
                    time.sleep(0.12) # configure
        finally:    
            self.cleanup()
            
    def cleanup(self):
        self.producer.close()
        self.camera.release()
        cv2.destroyAllWindows()
    

if __name__ == '__main__':
    broker_url = os.getenv('BROKER_URL', 'localhost:29092')
    topic = os.getenv('TOPIC', '')
    ImageProducer(broker_url, topic)
    
