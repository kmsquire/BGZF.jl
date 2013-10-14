module BGZF

using Zlib
import Zlib: z_stream, gz_header, Reader, libz, fillbuf, Z_OK
import Base: read, readbytes!, readuntil, close, eof, seek, position

export read, readbytes!, readuntil, close, eof, seek, position

type BGZFReader <: IO
    header::gz_header
    reader::Reader
    extra ::Array{Uint16}
end

function BGZFReader(io::IO; bufsize=65536)

    reader = Reader(io; bufsize=65536)
    
    header = gz_header()
    extra = Array(Uint16, 3)
    header.extra = pointer(extra)
    header.extra_max = 6

    fill_gzheader(header, reader, extra)
    
    BGZFReader(header, reader, extra)
end

function fill_gzheader(header::gz_header, reader::Reader, extra::Array{Uint16})

    # callable after inflateInit2() or inflateReset()
    ret = ccall((:inflateGetHeader, libz),
                Int32, (Ptr{z_stream}, Ptr{gz_header}),
                &reader.strm, &header)
    
    if ret != Z_OK
        error("BGZFReader: Error getting header")
    end


    # Really just want to read the header, but not yet implemented
    fillbuf(reader, 1)

    # Make sure this is a BGZF file

    if header.extra_len > 6
        error("Anticipated bug: this gzip compressed file contains more than one extra field, but we're only extracting the first.  Please file a bug report with the BGZF.jl package.")
    end
    
    extra8 = reinterpret(Uint8, extra)
    if header.extra_len < 6 ||
        extra8[1] != uint8(66) ||
        extra8[2] != uint8(67) ||
        extra[2] != 2
        error("BGZF extra field incorrect/missing; this does not seem to be a BGZF file")
    end
end

read{T}(r::BGZFReader, a::Array{T}) = read(r.reader, a)
read(r::BGZFReader, ::Type{Uint8}) = read(r.reader, Uint8)
readbytes!(r::BGZFReader, b::AbstractArray{Uint8}, nb=length(b)) = readbytes!(r.reader, b, nb)
readuntil(r::BGZFReader, delim::Uint8) = readuntil(r.reader, delim)
close(r::BGZFReader) = close(r.reader)
eof(r::BGZFReader) = eof(r.reader)
position(r::BGZFReader) = position(r.reader)

# seek using virtual offsets into file
function seek(r::BGZFReader, n::Integer)
    coffset = int64(n>>>16)
    uoffset = int64(n & 0xFFFF)
    seek(r, coffset, uoffset)
end

function seek(r::BGZFReader, coffset::Int64, uoffset::Int64)
    seek(r.reader.io, coffset)
    inflateReset(r.reader.strm)
    fill_gzheader(r.header, r.reader, r.extra)
    skip(r.reader, uoffset)
end

end # module
