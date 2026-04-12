#!/usr/bin/env python3
"""
Combined motor + fan control with speed control.

Motor (door):      IN3=GPIO18, IN4=GPIO19, EN=GPIO12
Fan (ventilator):  IN1=GPIO23, IN2=GPIO24, EN=GPIO25

Controls:
  ↑ / ↓       Motor forward / backward
  ← / →       Motor speed -10% / +10%
  + / -        Fan speed +10% / -10%
  0            Stop everything
  q            Quit

Fan is forward-only (reverse not safe for this motor).
"""
import sys, tty, termios
from gpiozero import Motor, PWMOutputDevice

# ----- GPIO setup -----
motor_in3 = 18
motor_in4 = 19
motor_en  = 12

fan_in1 = 23
fan_in2 = 24
fan_en  = 25

motor = Motor(forward=motor_in3, backward=motor_in4, enable=None)
motor_pwm = PWMOutputDevice(motor_en, frequency=1000)

fan = Motor(forward=fan_in1, backward=fan_in2, enable=None)
fan_pwm = PWMOutputDevice(fan_en, frequency=1000)

# ----- State -----
motor_speed = 0.5       # 0.0 – 1.0
motor_dir = 0           # -1 = backward, 0 = stopped, 1 = forward
fan_speed = 0.0         # 0.0 – 1.0  (0 = off)


def clamp(v):
    return max(0.0, min(1.0, round(v, 2)))


def apply_motor():
    motor_pwm.value = motor_speed
    if motor_dir == 1:
        motor.forward()
    elif motor_dir == -1:
        motor.backward()
    else:
        motor.stop()
        motor_pwm.value = 0


def apply_fan():
    if fan_speed > 0:
        fan.forward()
        fan_pwm.value = fan_speed
    else:
        fan.stop()
        fan_pwm.value = 0


def show():
    dir_label = {-1: "◀ REV", 0: "■ STOP", 1: "FWD ▶"}[motor_dir]
    fan_label = f"{int(fan_speed*100)}%" if fan_speed > 0 else "OFF"
    sys.stdout.write(
        f"\r\033[K  Motor: {dir_label} @ {int(motor_speed*100)}%   |   Fan: {fan_label}   "
    )
    sys.stdout.flush()


print("╔══════════════════════════════════════════════╗")
print("║  Motor + Fan control                         ║")
print("║  ↑/↓  motor fwd/rev   ←/→  motor speed       ║")
print("║  +/-  fan speed       0 = stop all  q=quit   ║")
print("╚══════════════════════════════════════════════╝")

fd = sys.stdin.fileno()
old = termios.tcgetattr(fd)
try:
    tty.setraw(fd)
    show()
    while True:
        ch = sys.stdin.read(1)

        if ch == 'q':
            break

        elif ch == '0':
            motor_dir = 0
            fan_speed = 0.0
            apply_motor()
            apply_fan()

        elif ch == '+' or ch == '=':
            fan_speed = clamp(fan_speed + 0.1)
            apply_fan()

        elif ch == '-' or ch == '_':
            fan_speed = clamp(fan_speed - 0.1)
            apply_fan()

        elif ch == '\x1b':
            sys.stdin.read(1)           # skip '['
            arrow = sys.stdin.read(1)
            if arrow == 'A':            # ↑  motor forward
                motor_dir = 1
                apply_motor()
            elif arrow == 'B':          # ↓  motor backward
                motor_dir = -1
                apply_motor()
            elif arrow == 'C':          # →  motor speed up
                motor_speed = clamp(motor_speed + 0.1)
                apply_motor()
            elif arrow == 'D':          # ←  motor speed down
                motor_speed = clamp(motor_speed - 0.1)
                apply_motor()
        else:
            # any other key = stop motor (safety)
            motor_dir = 0
            apply_motor()

        show()

finally:
    motor.stop()
    motor_pwm.value = 0
    fan.stop()
    fan_pwm.value = 0
    motor.close()
    motor_pwm.close()
    fan.close()
    fan_pwm.close()
    termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print("\nAll stopped. GPIO cleaned up.")
