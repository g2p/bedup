from cffi import FFI
import fcntl

ffi = FFI()
ffi.cdef('''
#define FS_IOC_FIEMAP ...

struct fiemap_extent {
    uint64_t fe_logical;  /* logical offset in bytes for the start of
                           * the extent from the beginning of the file */
    uint64_t fe_physical; /* physical offset in bytes for the start
                           * of the extent from the beginning of the disk */
    uint64_t fe_length;   /* length in bytes for this extent */
    uint32_t fe_flags;    /* FIEMAP_EXTENT_* flags for this extent */
    ...;
};

struct fiemap {
    uint64_t fm_start;  /* logical offset (inclusive) at
                         * which to start mapping (in) */
    uint64_t fm_length; /* logical length of mapping which
                         * userspace wants (in) */
    uint32_t fm_flags;          /* FIEMAP_FLAG_* flags for request (in/out) */
    uint32_t fm_mapped_extents; /* number of extents that were mapped (out) */
    uint32_t fm_extent_count;   /* size of fm_extents array (in) */
    struct fiemap_extent fm_extents[0]; /* array of mapped extents (out) */
    ...;
};

#define FIEMAP_MAX_OFFSET ...

#define FIEMAP_FLAG_SYNC                ... /* sync file data before map */
#define FIEMAP_FLAG_XATTR               ... /* map extended attribute tree */
#define FIEMAP_FLAGS_COMPAT             ...

#define FIEMAP_EXTENT_LAST              ... /* Last extent in file. */
#define FIEMAP_EXTENT_UNKNOWN           ... /* Data location unknown. */
#define FIEMAP_EXTENT_DELALLOC          ... /* Location still pending.
                                             * Sets EXTENT_UNKNOWN. */
#define FIEMAP_EXTENT_ENCODED           ... /* Data can not be read
                                             * while fs is unmounted */
#define FIEMAP_EXTENT_DATA_ENCRYPTED    ... /* Data is encrypted by fs.
                                             * Sets EXTENT_NO_BYPASS. */
#define FIEMAP_EXTENT_NOT_ALIGNED       ... /* Extent offsets may not be
                                             * block aligned. */
#define FIEMAP_EXTENT_DATA_INLINE       ... /* Data mixed with metadata.
                                             * Sets EXTENT_NOT_ALIGNED.*/
#define FIEMAP_EXTENT_DATA_TAIL         ... /* Multiple files in block.
                                             * Sets EXTENT_NOT_ALIGNED.*/
#define FIEMAP_EXTENT_UNWRITTEN         ... /* Space allocated, but
                                             * no data (i.e. zero). */
#define FIEMAP_EXTENT_MERGED            ... /* File does not natively
                                             * support extents. Result
                                             * merged for efficiency. */
#define FIEMAP_EXTENT_SHARED            ... /* Space shared with other
                                             * files. */

''')

lib = ffi.verify('''
#include <linux/fs.h>
#include <linux/fiemap.h>
''')


def fiemap(fd):
    """
    Gets a map of file extents.
    """

    count = 72
    fiemap_cbuf = ffi.new(
        'char[]',
        ffi.sizeof('struct fiemap')
        + count * ffi.sizeof('struct fiemap_extent'))
    fiemap_pybuf = ffi.buffer(fiemap_cbuf)
    fiemap_ptr = ffi.cast('struct fiemap*', fiemap_cbuf)
    assert ffi.sizeof(fiemap_cbuf) <= 4096

    while True:
        fiemap_ptr.fm_length = lib.FIEMAP_MAX_OFFSET
        fiemap_ptr.fm_extent_count = count
        fcntl.ioctl(fd, lib.FS_IOC_FIEMAP, fiemap_pybuf)
        if fiemap_ptr.fm_mapped_extents == 0:
            break
        for i in xrange(fiemap_ptr.fm_mapped_extents):
            extent = fiemap_ptr.fm_extents[i]
            yield extent
        fiemap_ptr.fm_start = extent.fe_logical + extent.fe_length


def same_extents(fd1, fd2):
    # Somehow CFFI does the right magic and this works.
    # Building namedtuples from the CData might be more foolproof nonetheless.
    return list(fiemap(fd1)) == list(fiemap(fd2))

