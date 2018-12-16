import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde
import matplotlib.cm as cm
from matplotlib import pyplot as plt

def raster(transactions, num_rows, **kwargs):
    """
    Creates a raster plot
    Parameters
    ----------
    transactions :  data frame
                    customer ID, sale date, sale value
    
    num_rows : int
               number of customers for whom to display raster
    Returns
    -------
    ax : an axis containing the raster plot
    """
    
    max_c_val = 50
    min_c_val = 0
    cbin_edges = np.arange(min_c_val,max_c_val, 1)
    
    cmap = cm.cool
    colors = cmap(np.linspace(0, 1, len(cbin_edges)))
    
    def get_rtick_color(sale_values):
        color_idx = []
        for sv in sale_values:
            try:
                ci = next(x[0] for x in enumerate(cbin_edges) if x[1] > sv)
            except: 
                ci = len(cbin_edges) - 1
            color_idx.append(colors[ci])
        return np.array(color_idx)

    
    custs = transactions.columns[0]
    sale_date = transactions.columns[1]
    sale_value = transactions.columns[2]
    
    transactions[sale_date] = pd.to_datetime(transactions[sale_date])
    
    min_date = np.min(transactions[sale_date])
        

    event_times_list = transactions.groupby(custs)[sale_date].apply(np.array)[:num_rows]
    sale_amount_list = transactions.groupby(custs)[sale_value].apply(np.array)[:num_rows]
    
    ax = plt.gca()
    max_event = np.datetime64(min_date)
    for ith, event_times in enumerate(event_times_list):
        c = get_rtick_color(list(sale_amount_list.iloc[ith]))
        plt.vlines(event_times, ith + .5, ith + 1.5, colors=c, linewidth = 5, **kwargs)
        
        if np.max(event_times) > max_event:
            max_event = np.max(event_times)
            
    plt.hlines(range(0+1,num_rows+1), min_date, max_event, linewidth = 1)
        
        
    plt.ylim(.5, len(event_times_list) + .5)
    plt.title('Customer Purchase Events')
    plt.xlabel('Time')
    plt.ylabel('Customer')
    ax.set_facecolor((0.98, 0.98, 0.98))
    
    #Color Bar
    
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(vmin=min_c_val, vmax=max_c_val))
    sm._A=[]
    cbar = plt.colorbar(sm)
    cbar.set_label('$ Value')
    
    plt.show();

def plot_scatter(dataframe, colx, coly, xlabel='', 
                 ylabel='', 
                 xlim=[0,15], ylim=[0,15], density=True): 
    """This function will plot a scatter plot of colx on the x-axis vs coly on the y-axis. 
    If you want to add a color to indicate the density of points, set density=True
    
    Args : 
        - dataframe (dataframe) : pandas dataframe containing the data of interest 
        - colx (str) : name of the column you want to put on the x axis 
        - coly (str) : same but for the y axis 
        - xlabel (str) : label to put on the x axis 
        - ylabel (str) : same for y axis 
        - xlim (list) : defines the range of x values displayed on the chart 
        - ylim (list) same for the y axis. 
        - density (bool) : set True to add color to indicate density of point. 
        
    """

    if not density : 
        plt.scatter(dataframe[colx].values, dataframe[coly].values)
    else:
        xvals = dataframe[colx].values
        yvals = dataframe[coly].values
        xy = np.vstack([xvals, yvals])
        z = gaussian_kde(xy)(xy)
        plt.scatter(xvals, yvals, c=z, s=10, edgecolor='')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.plot(np.linspace(xlim[0], xlim[1], 100), 
             np.linspace(ylim[0], ylim[1], 100), 
             color='black')
    plt.xlim(xlim)
    plt.ylim(ylim)
    plt.show();