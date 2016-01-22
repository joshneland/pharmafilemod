import multiprocessing as mp
import re
import itertools as it
import sys

# This is for updating big text files using even bigger AWS boxes where
# turn-around speed for development trumps memory savings.
# I did not bother getting fancy to use queues to keep the disk and CPU busy all the time.

# Create a lazy line iterator for a string.
def line_iter(string):
    return (m.group(2) or m.group(3) for m in re.finditer('((.*)\n|(.+)$)', string))

# reads entire file into memory, applies transform to each line, and writes back
def map_lines(file_list, func, append_ext=".modified"):
    global infile
    pool = mp.Pool(processes=None)  # uses all CPUs of the box
    for infile in file_list:

        with open(infile, "r") as fin:
            lines = line_iter(fin.read())  # read entire file into memory, and then pull each line through an iterator
            new_content = pool.imap(func, lines, chunksize=1000)

        outfile = infile + append_ext

        with open(outfile, "w") as fout:
            new_lines = it.imap(lambda s: s + '\n', new_content)
            fout.writelines(new_lines)


if __name__ == "__main__":

    file_list = [sys.argv[1]]
    func = lambda l: l + " bob!"
    append_ext = ".updated.txt"

    map_lines(file_list, func, append_ext)


