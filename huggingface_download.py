import os

from datasets import Features, Value, Sequence, Dataset, load_dataset
from huggingface_hub import snapshot_download


revision = "c74fa62247289ed31e407b6133d954d3c171db43"

local_dir = os.path.join(os.path.expanduser("~"), "Downloads", "motionx")

snapshot_download(
    repo_id="YuhongZhang/Motion-Xplusplus",
    repo_type="dataset",
    revision=revision,
    local_dir=local_dir,
    # local_files_only=True,
)

# motion_dataset = load_dataset(
#     "YuhongZhang/Motion-Xplusplus",
#     cache_dir=os.path.join(os.path.expanduser("~"), "Downloads", "motionx"),
#     features=None,
# )

# # print(squad_dataset["train"][0])
# print(motion_dataset)

# # Process the dataset - add a column with the length of the context texts
# dataset_with_length = squad_dataset.map(lambda x: {"length": len(x["context"])})

# # Process the dataset - tokenize the context texts (using a tokenizer from the 🤗 Transformers library)
# from transformers import AutoTokenizer

# tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

# tokenized_dataset = squad_dataset.map(lambda x: tokenizer(x["context"]), batched=True)
