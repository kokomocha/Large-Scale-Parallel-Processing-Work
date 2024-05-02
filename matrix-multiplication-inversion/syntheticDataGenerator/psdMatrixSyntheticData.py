import numpy as np

class generate_spd_matrix_data:
    def __init__(self, n, min_value, max_value, seed=42):
        self.n = n
        self.min_value = min_value
        self.max_value = max_value
        self.seed = seed
        np.random.seed(seed)
        matrix_vals = np.random.uniform(low=self.min_value, high=self.max_value, size=self.n * self.n)
        square_sym_matrix = matrix_vals.reshape(self.n, self.n)
        self.data = np.matmul(square_sym_matrix.transpose(), square_sym_matrix) + np.eye(self.n) * np.abs(np.min(np.linalg.eigvalsh(square_sym_matrix))) + 0.1

    def save_to_csv(self, file_name):
        np.savetxt(file_name, self.data, delimiter=",", fmt='%f')

if __name__ == '__main__':
    data_class = generate_spd_matrix_data(1000, 0, 5)
    data_class.save_to_csv("test")

