import os

from datasets import load_dataset


# Load a dataset and print the first example in the training set
squad_dataset = load_dataset(
    "YuhongZhang/Motion-Xplusplus",
    cache_dir=os.path.join(os.path.expanduser("~"), "Downloads", "motionx"),
)
# print(squad_dataset["train"][0])
print(squad_dataset)

# # Process the dataset - add a column with the length of the context texts
# dataset_with_length = squad_dataset.map(lambda x: {"length": len(x["context"])})

# # Process the dataset - tokenize the context texts (using a tokenizer from the 🤗 Transformers library)
# from transformers import AutoTokenizer

# tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

# tokenized_dataset = squad_dataset.map(lambda x: tokenizer(x["context"]), batched=True)
