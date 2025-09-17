import os
import time
import ctypes
import logging
import pyautogui
import pygetwindow as gw
import win32gui
from dotenv import load_dotenv


# Load environment variables (for secure credentials)
load_dotenv()

# Paths and constants
ZK_ACCESS_PATH = "C:\\ZKTeco\\ZKAccess3.5\\Access.exe"

# Check if Caps Lock is ON
if ctypes.windll.user32.GetKeyState(0x14) & 1:
    pyautogui.press("capslock")  # Turn it off
    time.sleep(1)

# Close ZKAccess if it is running 
os.system('TASKKILL /F /IM Access.exe')
    
# Start ZKAccess
os.startfile(ZK_ACCESS_PATH)
time.sleep(5)

# Logging admin
# ========== SETUP LOGGING ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ZKAccess credentials
USERNAME =os.getenv("ZKACCESS_USERNAME")
PASSWORD =os.getenv("ZKACCESS_PASSWORD")


# Helper function to resize and center windows
def resize_and_center_window_by_partial_class(partial_class_name, width, height, left, top):
    """
    Resizes and centers a window identified by a partial class name.

    Args:
        partial_class_name (str): A substring of the window class name to match.
        width (int): Desired width of the window.
        height (int): Desired height of the window.
        left (int): X-coordinate for positioning.
        top (int): Y-coordinate for positioning.

    Returns:
        bool: True if a matching window is found and resized, False otherwise.
    """
    def get_window_by_partial_class_name(partial_class_name):
        """Helper function to find the HWND of a window by partial class name."""
        def enum_handler(hwnd, results):
            if win32gui.IsWindowVisible(hwnd):
                current_class_name = win32gui.GetClassName(hwnd)
                if partial_class_name.lower() in current_class_name.lower():
                    results.append(hwnd)

        windows = []
        win32gui.EnumWindows(enum_handler, windows)
        return windows[0] if windows else None

    hwnd = get_window_by_partial_class_name(partial_class_name)
    if not hwnd:
        logger.error(f"No window found with class name containing: {partial_class_name}")
        return False

    # Resize and move the window using pygetwindow
    try:
        window = gw.Window(hwnd)
        window.resizeTo(width, height)
        window.moveTo(left, top)
        logger.info(f"Window resized and centered: Partial Class '{partial_class_name}', HWND '{hwnd}'")
        return True
    except Exception as e:
        logger.error(f"Error resizing and centering window: {e}")
        return False


def resize_and_center_window_by_title(title, width, height, left, top):
    """Resizes and centers a window identified by its title."""
    try:
        zkaccess_windows = gw.getWindowsWithTitle(title)

        if zkaccess_windows:
            window = zkaccess_windows[0]
            window.resizeTo(width, height)
            window.moveTo(left, top)
            time.sleep(3)  # Allow the window to resize
            logger.info(f"Window with title '{title}' resized to {width}x{height} and moved to ({left}, {top}).")
            return True
        else:
            logger.error(f"Window with title '{title}' not found.")
            return False
    except Exception as e:
        logger.error(f"Failed to resize and center window '{title}': {e}", exc_info=True)
        return False


def maximize_zkaccess_window():
    """Maximizes the ZKAccess window."""
    zkaccess_windows = gw.getWindowsWithTitle("ZKAccess3.5 Security System")
    if zkaccess_windows:
        zkaccess_window = zkaccess_windows[0]
        zkaccess_window.maximize()
        time.sleep(3)  # Allow the window to maximize
        return True
    else:
        logger.error("ZKAcess window not found.")
        return False


# Step 1: Automate ZKAcess Log Sync
def export_from_zk_access():
    """Open ZKAcess, login, and sync attendance logs to database."""
    if resize_and_center_window_by_partial_class("#32770", 127, 137, 741, 387):
        time.sleep(2)
        pyautogui.click(851,402)
        time.sleep(3)
        # return # Exit if the ZKAcess login window is not found
    

    # Resize and center the login window by class name
    if not resize_and_center_window_by_partial_class("WindowsForms10.Window.8.app", 400, 300, 760, 366):
        return  # Exit if the ZKAcess login window is not found

    # Automate login (adjust coordinates as needed)
    # pyautogui.click(930, 464)
    # pyautogui.doubleClick(915, 461)  # Username field
    # pyautogui.write(USERNAME)
    # pyautogui.doubleClick(934, 505)  # Password field
    # pyautogui.write(PASSWORD)
    pyautogui.click(952, 599)  # Login button
    time.sleep(5)  # Wait to log in



    if not maximize_zkaccess_window():
        return  # Exit if the ZKAcess window is not found

    # Go to Device tab and get all logs
    pyautogui.click(360, 23)  # Click on Device tab
    time.sleep(3)

    pyautogui.click(256, 94)  # Check all devices
    time.sleep(3)

    pyautogui.click(539, 66)  # Click on get logs
    time.sleep(3)

    if not resize_and_center_window_by_title("Get logs", 637, 258, 643, 390):
        return  # Exit if the Get logs window is not found

    # time.sleep(2)

    pyautogui.click(866, 458)  # click on get all logs
    time.sleep(2)

    pyautogui.click(1035, 618)  # Click on get
    time.sleep(750)

    pyautogui.click(1197, 612)  # Click on cancel
    time.sleep(2)
    # Close ZKAccess after log sync running 
    os.system('TASKKILL /F /IM Access.exe')

    
def main():
    """sync logs to database function."""
    export_from_zk_access()

# Run the main function
if __name__ == "__main__":
    main()
