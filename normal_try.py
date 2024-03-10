import numpy as np
import time

num_samples = 100_000_000

start_time = time.time()

# Using NumPy to generate random points efficiently
x = np.random.rand(num_samples)
y = np.random.rand(num_samples)
inside_points = (x**2 + y**2) < 1

# Count how many points fall inside the unit circle
count = np.sum(inside_points)

# Calculate Pi
pi = 4 * count / num_samples

end_time = time.time()
elapsed_time = end_time - start_time

print(f"Pi is roughly {pi:.6f}")
print(f"Calculation took {elapsed_time:.2f} seconds")
