'''read a csv file representing a table and write a restructured text simple
table'''
import sys
import csv

def get_out(out=None):
    '''
    return a file like object from different kinds of values
    None: returns stdout
    string: returns open(path)
    file: returns itself

    otherwise: raises ValueError
    '''

    if out is None:
        return sys.stdout
    elif isinstance(out, file):
        return out
    elif isinstance(out, basestring):
        return open(out)
    else:
        raise ValueError("out must be None, file or path")

def underline(title, underliner="=", endl="\n", out=None):
    '''
    write *title* *underlined* to *out*
    '''
    out = get_out(out)

    out.write(title)
    out.write(endl)
    out.write(underliner * len(title))
    out.write(endl * 2)

def separate(sizes, out=None, separator="=", endl="\n"):
    '''
    write the separators for the table using sizes to get the
    size of the longest string of a column
    '''
    out = get_out(out)

    for size in sizes:
        out.write(separator * size)
        out.write(" ")

    out.write(endl)

def write_row(sizes, items, out=None, endl="\n"):
    '''
    write a row adding padding if the item is not the
    longest of the column
    '''
    out = get_out(out)

    for item, max_size in zip(items, sizes):
        item_len = len(item)
        out.write(item)

        if item_len < max_size:
            out.write(" " * (max_size - item_len))

        out.write(" ")

    out.write(endl)

def process(in_=None, out=None, title=None):
    '''
    read a csv table from in and write the rest table to out
    print title if title is set
    '''
    handle = get_out(in_)
    out = get_out(out)

    reader = csv.reader(handle)

    rows = [row for row in reader if row]
    cols = len(rows[0])
    sizes = [0] * cols

    for i in range(cols):
        for row in rows:
            row_len = len(row[i])
            max_len = sizes[i]

            if row_len > max_len:
                sizes[i] = row_len

    if title:
        underline(title)

    separate(sizes, out)
    write_row(sizes, rows[0], out)
    separate(sizes, out)

    for row in rows[1:]:
        write_row(sizes, row, out)

    separate(sizes, out)

def run():
    '''
    run as a command line program
    usage: program.py path_to_table [title]
    '''
    path = sys.argv[1]

    title = None

    if len(sys.argv) > 2:
        title = sys.argv[2]

    process(path, title=title)

if __name__ == "__main__":
    run()