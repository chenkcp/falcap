"""
Custom statistical functions to replace numpy for AWS Lambda deployment.
Provides equivalent functionality without external dependencies.
"""
import statistics
from typing import List, Union

def percentile(data: List[float], percentile: float, method: str = "linear") -> float:
    """
    Calculate percentile of data similar to numpy.percentile()
    
    Args:
        data: List of numeric values
        percentile: Percentile to calculate (0-100)
        method: Interpolation method ("higher", "lower", "linear")
    
    Returns:
        Calculated percentile value
    """
    if not data:
        raise ValueError("Data cannot be empty")
    
    sorted_data = sorted(data)
    n = len(sorted_data)
    
    if percentile == 0:
        return sorted_data[0]
    if percentile == 100:
        return sorted_data[-1]
    
    # Calculate position
    pos = (percentile / 100) * (n - 1)
    lower_idx = int(pos)
    upper_idx = min(lower_idx + 1, n - 1)
    
    if method == "lower":
        return sorted_data[lower_idx]
    elif method == "higher":
        return sorted_data[upper_idx]
    else:  # linear interpolation (default)
        if lower_idx == upper_idx:
            return sorted_data[lower_idx]
        
        fraction = pos - lower_idx
        lower_val = sorted_data[lower_idx]
        upper_val = sorted_data[upper_idx]
        return lower_val + fraction * (upper_val - lower_val)

def ptp(data: List[float]) -> float:
    """
    Calculate peak-to-peak (range) of data similar to numpy.ptp()
    
    Args:
        data: List of numeric values
    
    Returns:
        Range (max - min) of the data
    """
    if not data:
        return 0.0
    
    return max(data) - min(data)

def array_filter(data: List[float], condition_func) -> List[float]:
    """
    Filter array based on condition function
    
    Args:
        data: List of numeric values  
        condition_func: Function that returns boolean for each element
    
    Returns:
        Filtered list
    """
    return [x for x in data if condition_func(x)]