pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
pip install pdfplumber
pip install datasets

rclone listremotes

rclone lsf gdrive: --dirs-only

rclone copy ~/datasets/motion-x/video gdrive:motion-x/video --progress --transfers=8 --checkers=16 --tpslimit=10 --drive-chunk-size=64M --log-level=INFO

```
from huggingface_hub import HfApi

api = HfApi()
info = api.dataset_info("robfiras/loco-mujoco-datasets")
print(info)  # in bytes
```