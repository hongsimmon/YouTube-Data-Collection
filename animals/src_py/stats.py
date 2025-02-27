import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

def format_axis_labels(value, pos):
    """Format axis labels to be more readable"""
    if value >= 1e9:
        return f'{value/1e9:.1f}B'
    elif value >= 1e6:
        return f'{value/1e6:.1f}M'
    elif value >= 1e3:
        return f'{value/1e3:.1f}K'
    else:
        return f'{value:.1f}'

def fit_distribution(data):
    """Fit statistical distributions and return the best fit"""
    # Remove zeros and infinities
    data = data[~np.isinf(data)]
    data = data[data != 0]
    
    distributions = {
        'norm': stats.norm,
        'lognorm': stats.lognorm,
        'expon': stats.expon
    }
    
    best_fit = {'name': None, 'params': None, 'sse': float('inf')}
    
    for dist_name, dist in distributions.items():
        try:
            params = dist.fit(data)
            # Calculate error
            hist, bins = np.histogram(data, bins=50, density=True)
            bin_centers = (bins[:-1] + bins[1:]) / 2
            sse = np.sum((hist - dist.pdf(bin_centers, *params)) ** 2)
            
            if sse < best_fit['sse']:
                best_fit = {
                    'name': dist_name,
                    'params': params,
                    'sse': sse,
                    'dist': dist
                }
        except Exception as e:
            continue
            
    return best_fit

def create_raw_distribution_plots(df_channels):
    """Create distribution plots showing raw frequency counts with best fit line"""
    metrics = {
        'Subscribers': {'data': df_channels['Subscribers'], 'color': '#87CEEB'},
        'Video Views': {'data': df_channels['Video Views'], 'color': '#90EE90'},
        'Video Count': {'data': df_channels['Video Count'], 'color': '#FA8072'}
    }
    
    plt.rcParams['figure.figsize'] = [15, 15]
    plt.rcParams['axes.grid'] = True
    plt.rcParams['grid.alpha'] = 0.3
    
    fig = plt.figure()
    
    for idx, (metric_name, metric_info) in enumerate(metrics.items(), 1):
        data = metric_info['data']
        
        # Create subplot
        ax = plt.subplot(3, 1, idx)
        
        # Plot histogram with raw counts
        counts, bins, _ = plt.hist(data, bins=50, color=metric_info['color'], 
                                 alpha=0.6, label='Actual Distribution')
        
        # Fit distribution
        best_fit = fit_distribution(data)
        
        if best_fit['name']:
            # Convert fitted probability density to frequency
            bin_width = bins[1] - bins[0]
            x = np.linspace(min(data), max(data), 100)
            y = best_fit['dist'].pdf(x, *best_fit['params'])
            # Scale the density to match the frequency scale
            y = y * len(data) * bin_width
            
            plt.plot(x, y, 'r-', lw=2, 
                    label=f'Best Fit ({best_fit["name"]})\nSSE: {best_fit["sse"]:.2e}')
        
        # Add statistical annotations
        add_statistical_annotations(ax, data, counts)
        
        # Format axes
        plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(format_axis_labels))
        plt.title(f'{metric_name} Distribution (Raw Counts)\n', fontsize=12, pad=20)
        plt.xlabel(f'{metric_name}', fontsize=10)
        plt.ylabel('Frequency (Number of Channels)', fontsize=10)
        plt.legend(fontsize=8)
    
    plt.tight_layout(pad=3.0)
    return fig

def add_statistical_annotations(ax, data, counts):
    """Add statistical annotations to the plot"""
    stats_text = (
        f'Mean: {format_axis_labels(np.mean(data), None)}\n'
        f'Median: {format_axis_labels(np.median(data), None)}\n'
        f'Total Channels: {len(data):,}'
    )
    
    plt.text(0.95, 0.95, stats_text,
             transform=ax.transAxes,
             verticalalignment='top',
             horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

def main():
    try:
        # Load data
        df_channels = pd.read_csv('../data_csv/channels_2024-10-15.csv')
        
        # Convert string columns to numeric
        df_channels['Subscribers'] = pd.to_numeric(df_channels['Subscribers'].str.replace(',', ''), errors='coerce')
        df_channels['Video Views'] = pd.to_numeric(df_channels['Video Views'].str.replace(',', ''), errors='coerce')
        df_channels['Video Count'] = pd.to_numeric(df_channels['Video Count'].str.replace(',', ''), errors='coerce')
        
        # Create raw distribution plots with best fit
        fig = create_raw_distribution_plots(df_channels)
        
        # Save the figure
        save_path = '../data_graph/2024-10-15/raw_distributions_with_fit.png'
        fig.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close(fig)
        
        print(f"Raw distribution plots with best fit lines saved to {save_path}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()