def make_data_dirs(data_in_directory, data_out_directory, cmf_directory):
    import os

    print("This is //////// ", os.getcwd())
    os.makedirs(data_in_directory, exist_ok=True)
    os.makedirs(data_out_directory, exist_ok=True)
    os.makedirs(cmf_directory, exist_ok=True)
    print("\\\\\\", os.listdir())
    print("\\\\\\", os.listdir("data/input/"))
    print("\\\\\\", os.listdir("data/output"))
