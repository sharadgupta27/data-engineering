import os
import sys
import urllib.request
from pathlib import Path


def setup_hadoop_for_windows(hadoop_version: str = "3.3.6", hadoop_home: str = "C:/hadoop") -> bool:
    """
    Download winutils.exe and hadoop.dll required for running Apache Spark on Windows.

    Args:
        hadoop_version: Hadoop version to download binaries for (default: "3.3.6").
        hadoop_home: Path where Hadoop binaries will be installed (default: "C:/hadoop").

    Returns:
        True if setup succeeded (or not on Windows), False if any binary is still missing.
    """
    if sys.platform != 'win32':
        return True

    hadoop_home_path = Path(hadoop_home)
    bin_dir = hadoop_home_path / 'bin'
    bin_dir.mkdir(parents=True, exist_ok=True)

    base_url = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{hadoop_version}/bin"
    needed = {
        'winutils.exe': base_url + '/winutils.exe',
        'hadoop.dll':   base_url + '/hadoop.dll',
    }

    for fname, url in needed.items():
        dest = bin_dir / fname
        if not dest.exists():
            print(f"{fname} not found — downloading ...")
            try:
                urllib.request.urlretrieve(url, str(dest))
                print(f"  Downloaded to {dest}")
            except Exception as e:
                print(f"  Auto-download failed: {e}")
                print(f"  Manual fix: download {fname} from https://github.com/cdarlint/winutils")
                print(f"  and place it at: {dest}")
        else:
            print(f"{fname} already present at {dest}")

    if (bin_dir / 'winutils.exe').exists() and (bin_dir / 'hadoop.dll').exists():
        os.environ['HADOOP_HOME'] = str(hadoop_home_path)
        # Prepend bin dir so the JVM finds hadoop.dll on startup
        os.environ['PATH'] = str(bin_dir) + ';' + os.environ.get('PATH', '')
        print(f"\nHADOOP_HOME = {os.environ['HADOOP_HOME']}")
        print("Both winutils.exe and hadoop.dll are in place. Restart kernel if SparkSession was already created.")
        return True
    else:
        print("WARNING: one or more Hadoop binaries are still missing. Parquet writes will fail.")
        return False


if __name__ == "__main__":
    setup_hadoop_for_windows()
