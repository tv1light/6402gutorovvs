def generate_autocorr(data, column):
    for lag in range(1, len(data) + 1):
        yield data[column].autocorr(lag=lag)