"""Join Colab results with the exported message shards -> compact CSV.gz for `python -m dixo.sentiment import`.
Needs pyarrow (run on a workstation, not prod).
  python tools/sentiment_prepare.py <dir with messages_*.parquet> <dir with sentiment_*.parquet> out.csv.gz
"""
import glob, os, sys
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pyarrow as pa

src, res, out = sys.argv[1:4]
writer = None
sink = pa.CompressedOutputStream(out, "gzip")
n = 0
for rp in sorted(glob.glob(os.path.join(res, "sentiment_*.parquet"))):
    k = os.path.basename(rp).split("_")[1].split(".")[0]
    m = pq.read_table(os.path.join(src, f"messages_{k}.parquet"), columns=["id", "author_id", "day"])
    r = pq.read_table(rp, columns=["id", "label", "p_neg", "p_pos"])
    t = r.join(m, "id")
    t = t.select(["id", "author_id", "day", "label", "p_neg", "p_pos"])
    t = t.set_column(4, "p_neg", pc.round(t["p_neg"].cast(pa.float64()), 4))
    t = t.set_column(5, "p_pos", pc.round(t["p_pos"].cast(pa.float64()), 4))
    if writer is None:
        writer = pcsv.CSVWriter(sink, t.schema)
    writer.write_table(t); n += t.num_rows
    print(k, t.num_rows, "of", r.num_rows, flush=True)
writer.close(); sink.close()
print("rows", n)
