import paramiko
import os

# === Configuration ===
local_dir = r"D:\Revenue\media\report"
remote_dir = "/home/iccsadmin/Revenue_Data"
hostname = "172.20.122.231"
port = 22
username = "iccsadmin"
password = "Xs0a0@bdpkgo"  # Recommended: Use SSH keys or environment variables

def upload_directory_sftp(local_dir, remote_dir):
    try:
        # SSH connection setup
        transport = paramiko.Transport((hostname, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Ensure remote directory exists
        try:
            sftp.chdir(remote_dir)
        except IOError:
            sftp.mkdir(remote_dir)
            sftp.chdir(remote_dir)

        # Upload files
        for item in os.listdir(local_dir):
            local_path = os.path.join(local_dir, item)
            remote_path = f"{remote_dir}/{item}"

            if os.path.isfile(local_path):
                print(f"Uploading {local_path} to {remote_path}")
                sftp.put(local_path, remote_path)

        sftp.close()
        transport.close()
        print("Upload completed successfully!")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    upload_directory_sftp(local_dir, remote_dir)
