from time import perf_counter


def main():
    t_start = perf_counter()
    t_end = perf_counter()
    print(t_start - t_end)

if __name__ == "__main__":
    main()