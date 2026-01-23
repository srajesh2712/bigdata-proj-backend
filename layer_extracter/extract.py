# demucs_run_input.py
import subprocess
import shutil
import sys
from pathlib import Path

# Ask for file name
audio = input("Enter the audio file name (e.g., Nagomo.mp3): ").strip()
audio_path = Path(audio)

# Check Demucs availability
if shutil.which("demucs") is None:
    print("❌ Demucs not found. Install it with: pip install demucs")
    sys.exit(1)

# Check file exists
if not audio_path.exists():
    print(f"❌ File not found: {audio_path}")
    sys.exit(1)

# Make output directory same as file name (without extension)
out_dir = audio_path.stem  # e.g. "Nagomo"
Path(out_dir).mkdir(exist_ok=True)

# Build demucs command
cmd = [
    "demucs",
    "-o", out_dir,             # output folder = file name
    "--two-stems=vocals",      # optional (vocals vs instruments)
    str(audio_path)
]

print(f"🎵 Running Demucs on {audio_path.name} ...")
try:
    subprocess.run(cmd, check=True)
    print(f"✅ Done! Check output in: {Path(out_dir).resolve()}")
except subprocess.CalledProcessError as e:
    print("❌ Demucs failed:", e)

