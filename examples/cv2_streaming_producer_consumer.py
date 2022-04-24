import cv2
import faust
import numpy as np


class Frame(faust.Record):
    frame_idx: int
    frame: np.ndarray


app = faust.App('stream-app', broker='kafka://localhost:9092')
topic = app.topic('streaming', value_type=Frame)
camera = cv2.VideoCapture('tcp://192.168.1.107:5000')


@app.agent(topic)
async def stream(greetings):
    async for greeting in greetings:
        print(f'Hello from {np.asarray(greeting.frame).shape} of {greeting.frame_idx}')


def serialize_frame(frame):
    return frame.tolist()


def cv_read():
    return serialize_frame(camera.read()[1])


@app.timer(interval=1.0)
async def example_sender(app):
    await stream.send(
        value=Frame(frame_idx='Faust', frame=cv_read()),
    )
