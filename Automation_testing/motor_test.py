#!/usr/bin/env python3
"""
Motor test — H-bridge on RPi (gpiozero)
  IN3 = GPIO18, IN4 = GPIO19, EN = GPIO12
  ↑ = motor up, ↓ = motor down, any other key = stop, q = quit
"""
import sys, tty, termios
from gpiozero import Motor

motor = Motor(forward=18, backward=19, enable=12)

print("Motor test ready.  ↑=up  ↓=down  any key=stop  q=quit")

fd = sys.stdin.fileno()
old = termios.tcgetattr(fd)
try:
    tty.setraw(fd)
    while True:
        ch = sys.stdin.read(1)
        if ch == 'q':
            break
        if ch == '\x1b':
            sys.stdin.read(1)           # skip '['
            arrow = sys.stdin.read(1)
            if arrow == 'A':            # ↑
                motor.forward()
            elif arrow == 'B':          # ↓
                motor.backward()
        else:
            motor.stop()
finally:
    motor.stop()
    motor.close()
    termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print("\nMotor stopped. GPIO cleaned up.")
