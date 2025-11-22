import sys
import os
from PyInstaller.utils.hooks import collect_data_files

# Determine if running as a script or frozen
if getattr(sys, 'frozen', False):
    # Running as compiled executable
    base_dir = sys._MEIPASS
else:
    # Running as script
    base_dir = os.path.abspath(".")

block_cipher = None

a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('templates', 'templates'),
    ],
    hiddenimports=[
        'openpyxl.cell._writer',
        'pandas._libs.tslibs.base',
        'flask'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ServerMergeTool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep console for logs, set to False for GUI-only
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
