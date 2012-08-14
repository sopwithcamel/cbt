src_files = Glob('src/*.cpp')
prefix = '/usr/local/lib'

env = Environment()
env.Append(CCFLAGS = ['-g','-O2','-Wall'])
cbtlib = env.SharedLibrary('cbt', src_files)

# install targets
env.Alias('install', Install(prefix, cbtlib))
