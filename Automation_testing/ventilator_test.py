#!/usr/bin/env python3
"""
Ventilator test — single-direction 12V DC motor via H-bridge on RPi5
  IN1 = GPIO23, IN2 = GPIO24, EN = GPIO25
  ↑ = on, ↓ = off, q = quit

  WARNING: only one direction is safe — reverse is disabled.
"""
import sys, tty, termios
from gpiozero import Motor

motor = Motor(forward=23, backward=24, enable=25)

print("Ventilator test ready.  ↑=on  ↓=off  q=quit")

fd = sys.stdin.fileno()
old = termios.tcgetattr(fd)
try:
    tty.setraw(fd)
    while True:
        ch = sys.stdin.read(1)
        if ch == 'q':
            break
        if ch == '\x1b':
            sys.stdin.read(1)
            arrow = sys.stdin.read(1)
            if arrow == 'A':        # ↑  on (forward only)
                motor.forward()
            elif arrow == 'B':      # ↓  off
                motor.stop()
        else:
            motor.stop()
finally:
    motor.stop()
    motor.close()
    termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print("\nVentilator stopped. GPIO cleaned up.")
