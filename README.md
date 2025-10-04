# Script to read NASA Data .nc files

![Screenshot of command line output](https://github.com/user-attachments/assets/47522d91-83e0-4386-880c-5f77cc301288)

## Steps
1. `pip install -r requirements.txt`
2. `python nc-read`
3. Pass path to .nc file
4. Select desired output

## Interpretation Guidance
## 🛰️ PACE/OCI Data Products for Urban Air Quality (Health Assessment)

The most relevant data products for assessing healthy/unhealthy areas in an urban environment are those related to **aerosols**, as these particles are direct indicators of air pollution and its impact on public health.

***

### 1. Aerosol Optical Properties (AER)

These products provide a comprehensive set of parameters to characterize the airborne particles, which is vital for distinguishing pollution types (e.g., smoke vs. dust) and their potential toxicity.

| Product Name | Short Name | Data Level | Real-time (NRT) Status |
| :--- | :--- | :--- | :--- |
| **Global Mapped Aerosol Optical Properties, UAA** | `PACE_OCI_L3M_AER_UAA` | L3M (Mapped) | Standard |
| **Global Mapped Aerosol Optical Properties, UAA (NRT)** | `PACE_OCI_L3M_AER_UAA_NRT` | L3M (Mapped) | Near Real-time |
| **Regional Aerosol Optical Properties, UAA** | `PACE_OCI_L2_AER_UAA` | L2 (Regional) | Standard |
| **Regional Aerosol Optical Properties, UAA (NRT)** | `PACE_OCI_L2_AER_UAA_NRT` | L2 (Regional) | Near Real-time |

***

### 2. Aerosol Optical Thickness (AOT)

AOT is the fundamental measurement for aerosol loading. High AOT values are a direct proxy for high concentrations of Particulate Matter ($\text{PM}_{2.5}$ and $\text{PM}_{10}$) and thus, poor air quality.

| Product Name | Short Name | Data Level | Real-time (NRT) Status |
| :--- | :--- | :--- | :--- |
| **Global Mapped Aerosol Optical Thickness** | `PACE_OCI_L3M_AOT` | L3M (Mapped) | Standard |
| **Global Mapped Aerosol Optical Thickness (NRT)** | `PACE_OCI_L3M_AOT_NRT` | L3M (Mapped) | Near Real-time |
| **Global Binned Aerosol Optical Thickness** | `PACE_OCI_L3B_AOT` | L3B (Binned) | Standard |
| **Global Binned Aerosol Optical Thickness (NRT)** | `PACE_OCI_L3B_AOT_NRT` | L3B (Binned) | Near Real-time |

***

### 3. Aerosol Index (UVAI)

The Near-UV Aerosol Index helps to detect the presence of strongly absorbing aerosols (like smoke, black carbon, and dust) that are lofted high in the atmosphere, aiding in tracking pollution sources and transport.

| Product Name | Short Name | Data Level | Real-time (NRT) Status |
| :--- | :--- | :--- | :--- |
| **Global Mapped Aerosol Index, UAA** | `PACE_OCI_L3M_UVAI_UAA` | L3M (Mapped) | Standard |
| **Global Mapped Aerosol Index, UAA (NRT)** | `PACE_OCI_L3M_UVAI_UAA_NRT` | L3M (Mapped) | Near Real-time |
| **Regional Aerosol Index, UAA** | `PACE_OCI_L2_UVAI_UAA` | L2 (Regional) | Standard |
| **Regional Aerosol Index, UAA (NRT)** | `PACE_OCI_L2_UVAI_UAA_NRT` | L2 (Regional) | Near Real-time |