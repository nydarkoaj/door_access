import pygetwindow as gw
import pyautogui
import time
import win32gui
from datetime import datetime


# titles = gw.getAllTitles()
# result = ""
# for title in titles:
#     if title.startswith("C"):
#         result = title
#
# zkaccess_window = gw.getWindowsWithTitle(result)[0]
# print(zkaccess_window)
# zkaccess_window.maximize()





# Wait for 10 seconds to give you time to move your mouse
time.sleep(20)
# Prints the current position of the mouse
print(pyautogui.position())

# Prints screen resolution
#print(pyautogui.size())




def list_all_window_classes():
    """Lists all visible windows along with their class names."""

    def enum_handler(hwnd, results):
        if win32gui.IsWindowVisible(hwnd):
            class_name = win32gui.GetClassName(hwnd)
            title = win32gui.GetWindowText(hwnd)
            results.append((hwnd, class_name, title))

    windows = []
    win32gui.EnumWindows(enum_handler, windows)

    for window in windows:
        hwnd, class_name, title = window
        print(f"HWND: {hwnd}, CLASS NAME: {class_name}, TITLE: {title}")

# list_all_window_classes()


# hwnd = 788342
# window = gw.Window(hwnd)
# print("\n")
# print(f"WINDOW SIZE: {window.size}")
# print(f"TOPLEFT: {window.topleft}")
# print(f"LEFT: {window.left}")
# print(f"TOP: {window.top}")







