import math

def calculate_sample_size(p1=0.05, relative_mde=0.10, alpha=0.05, power=0.80):
    p2 = p1 * (1 + relative_mde)
    delta = abs(p2 - p1)
    p_avg = (p1 + p2) / 2.0

    z_alpha = 1.96
    z_beta = 0.8416

    numerator = (z_alpha * math.sqrt(2 * p_avg * (1 - p_avg)) + z_beta * math.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
    denominator = delta ** 2
    n_per_variant = math.ceil(numerator / denominator)
    return n_per_variant, p2

def get_required_sample_size(p1=0.05, relative_mde=0.10, alpha=0.05, power=0.80) -> int:
    n_per_group, _ = calculate_sample_size(p1, relative_mde, alpha, power)
    return int(n_per_group * 2)

if __name__ == '__main__':
    p1 = 0.05
    relative_mde = 0.10
    n_per_group, target_p2 = calculate_sample_size(p1, relative_mde)
    total_required = n_per_group * 2
    print(f'Baseline Conversion (Control): {p1 * 100:.2f}%')
    print(f'Target Conversion (Treatment): {target_p2 * 100:.2f}% (Lift: +{relative_mde * 100:.0f}%)')
    print(f'Sample Size Per Group:          {n_per_group:,} users')
    print(f'TOTAL Sessions Required:        {total_required:,} sessions')
