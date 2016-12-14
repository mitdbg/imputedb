using DataFrames

version = 2
othp = 0.02
telp = 0.11
stp = 0.00
infile = "acs.txt"
outfile = "acs_dirty$version.txt"
f = open(outfile, "w")

df = readtable(infile, separator=',', header=false)

iST = 1
iTEL = 14
d = ncol(df) # 37
n = nrow(df) # 671153

# fill in p
p = othp * ones(d)
p[iTEL] = telp
p[iST] = stp

for i in 1:n
    row = Array{Any}(df[i,:]) # sketchy to allow us to print ""
    for j in 1:d
        if rand() < p[j]
            row[j] = ""
        end
    end

    println(f, join(row, ","))
end

close(f)
