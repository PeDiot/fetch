from typing import List


def create_batches(input_list: List, batch_size: int) -> List[List]:
    batches = []

    for i in range(0, len(input_list), batch_size):
        batch = input_list[i : i + batch_size]
        batches.append(batch)

    return batches
