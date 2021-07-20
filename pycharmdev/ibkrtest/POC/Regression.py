import matplotlib.pyplot as plt

import wtsdblib
import pandas
import numpy as np
import matplotlib.pyplot as plt

def demo_regression():
    x = [val for val in range(25)]
    y1 = [val+2 for val in x]
    #y2= np.exp(x)
    y2 = [1 / (val + 2) for val in x]
    print (x)
    print (y1)
    # Linear regression for x and y1
    m,c = np.polyfit(x,y1,1)
    print (m,c)
    y = [m * xval + c for xval in x]
    print (y)
    correlation = np.corrcoef(y,y1)
    r_squared = correlation[0,1] ** 2
    final_output = f"Start = {c}, Slope = {m}, R square = {r_squared}"
    print (final_output)
    plt.plot(x,y1)
    plt.plot(x,y)
    plt.title(final_output )
    plt.xlabel('x')
    plt.ylabel('y & y1')
    plt.show()
    input("Press Enter to continue...")
    # Linear regression for x and y2
    print (y2)
    m,c = np.polyfit(x,y2,1)
    print (m,c)
    y = [m * xval + c for xval in x]
    print (y)
    correlation = np.corrcoef(y,y2)
    r_squared = correlation[0,1] ** 2
    final_output = f"Start = {c}, Slope = {m}, R square = {r_squared}"
    print (final_output)
    plt.plot(x,y2)
    plt.plot(x,y)
    plt.title(final_output )
    plt.xlabel('x')
    plt.ylabel('y & y2')
    plt.show()
    input("Press Enter to continue...")
    # Exponential regression for x and y2
    print (y2)
    log_y2 = np.log(y2)
    m,c = np.polyfit(x,log_y2,1)
    print (m,c)
    y = [np.exp(m * xval) * np.exp(c) for xval in x]
    print (y)
    correlation = np.corrcoef(y,y2)
    r_squared = correlation[0,1] ** 2
    final_output = f"Start = {c}, Slope = {m}, R square = {r_squared}"
    print (final_output)
    plt.plot(x,y2)
    plt.plot(x,y)
    plt.title(final_output )
    plt.xlabel('x')
    plt.ylabel('y & y2')
    plt.show()
    input("Press Enter to continue...")




if __name__ == '__main__':
    demo_regression()

