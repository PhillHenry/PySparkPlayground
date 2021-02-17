import random

NUM_SAMPLES = 100000000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1


def calculate_pi(sc):
    count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
    pi = 4 * count / NUM_SAMPLES
    print("Pi is roughly ", pi)
