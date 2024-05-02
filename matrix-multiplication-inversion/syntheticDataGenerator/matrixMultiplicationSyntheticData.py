import numpy as np

class generate_matrix_data:
    def __init__(self, row, col, min_value, max_value, seed=42):
        # Note, not programmed here but need to make sure that row and col values are not negative
        self.row = row
        self.col = col
        self.min_value = min_value
        self.max_value = max_value
        self.seed = seed
        np.random.seed(seed)
        matrix_vals = np.random.uniform(low = self.min_value, high = self.max_value, size = self.row * self.col)
        self.data = matrix_vals.reshape(self.row, self.col)

    def save_to_csv(self, file_name):
        np.savetxt(file_name, self.data, delimiter=",", fmt='%f')

    def calc_ATA_val(self, row, col):
        return np.dot(self.data[:, row - 1], self.data[:, col - 1])

if __name__ == '__main__':
    data_class = generate_matrix_data(3, 5, 0, 5)
    data_class.save_to_csv("test")

    print(np.matmul(data_class.data.transpose(), data_class.data))
    print(data_class.calc_ATA_val(3,2))
