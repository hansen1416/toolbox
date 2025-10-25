pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
pip install pdfplumber
pip install datasets

rclone listremotes

rclone lsf gdrive: --dirs-only

rclone size gdrive:motion-x/video --human-readable

rclone copy ~/datasets/motion-x/video gdrive:motion-x/video --progress --transfers=8 --checkers=16 --tpslimit=10 --drive-chunk-size=64M --log-level=INFO --checksum

```
from huggingface_hub import HfApi

api = HfApi()
info = api.dataset_info("robfiras/loco-mujoco-datasets")
print(info)  # in bytes
```



I installed nvidia-container-toolkut, but I still got this error: docker: Error response from daemon: could not select device driver "" with capabilities: [[gpu]]

Quick fix:

# 1) Verify host driver is working
nvidia-smi

# 2) Register NVIDIA runtime with Docker and make it default
sudo nvidia-ctk runtime configure --runtime=docker --set-as-default
sudo systemctl restart docker

# 3) Confirm Docker sees the runtime
docker info | grep -i -E 'Runtimes|Default Runtime'
# Expect: Runtimes: nvidia runc ...  and  Default Runtime: nvidia

# 4) Test inside a CUDA container (use an available tag)
docker run --rm --gpus all nvidia/cuda:12.5.0-base-ubuntu22.04 nvidia-smi
