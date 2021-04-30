import os
from secrets import token_hex

MB = 1024 * 1024

LIMIT = 5 * MB


def write_items_to_file(filename, items):
    with open(filename, "w") as fo:
        for item in items:
            fo.write(item[1])


if __name__ == "__main__":
    filename = "./hash_catid_count.csv"

    # Step 1: Partition
    cnt = 0
    items = []
    sorted_chunk = []
    with open(filename, "r") as fi:
        for line in fi:
            cnt += len(line)
            # strcuture of item: 
            # item as a tuple 
            # ( object_id, data )
            items.append(
                (
                    int(line[: line.find("\t")]),
                    line,
                )
            )

            if cnt > LIMIT:
                items.sort(key=lambda x: x[0])
                out = token_hex(5) + ".tmp"
                write_items_to_file(out, items)
                sorted_chunk.append(out)
                items = []
                cnt = 0
        if cnt:
            items.sort(key=lambda x: x[0])
            out = token_hex(5) + ".tmp"
            write_items_to_file(out, items)
            sorted_chunk.append(out)

    # Step 2: Merge
    try:
        chunks = [open(fname, "r") for fname in sorted_chunk]

        with open("output.csv", "w") as fo:
            items = [None for _ in range(len(chunks))]
            while 1:
                for i in range(len(chunks)):
                    if items[i] is None:
                        try:
                            line = chunks[i].readline()
                            items[i] = (
                                int(line[: line.find("\t")]),
                                line,
                            )
                        except:
                            pass

                new_items, new_chunks = [], []
                for i in range(len(items)):
                    if items[i] is None:
                        chunks[i].close()
                        os.remove(chunks[i].name)
                    else:
                        new_items.append(items[i])
                        new_chunks.append(chunks[i])

                items, chunks = new_items, new_chunks

                if len(items) < 2:
                    break

                imin = 0
                for i in range(2, len(items)):
                    if items[i][0] < items[imin][0]:
                        imin = i

                fo.write(items[imin][1])

                items[imin] = None

            if items:
                fo.write(items[0][1])
                for line in chunks[0]:
                    fo.write(line)

    except Exception as err:
        print("Error when merge:", err)
    finally:
        for f in chunks:
            if f:
                if not f.closed:
                    f.close()
            if os.path.exists(f.name):
                os.remove(f.name)
