# Heavy Metal Data 🎸

This project consists of three main steps: web scraping, data analysis, and RNN model training. The goal is to collect, analyze, and classify heavy metal albums based on their names.

---

## 1. Web Scraping 🕸️

In this step, we collected data from various sources:  

- **Primary dataset:** Album names and genres (Black, Death, Doom Metal) scraped from the blog [Angry Metal Guy](https://www.angrymetalguy.com).  
- **Test dataset:** Additional albums collected from other websites to evaluate the model performance.  

---

## 2. Data Analysis 📊

Here, we analyzed the data collected from Angry Metal Guy:  

- Identified the most reviewed genres.  
- Evaluated the best and worst bands based on user reviews.  
- Explored patterns in album naming across genres.  

---

## 3. RNN Model Training 🤖

In this step, we trained a **Recurrent Neural Network (RNN)** model using **Transfer Learning** with the **FastAI Python library**.  

- The model predicts the genre (Black or Death Metal) based only on album names.  
- The test dataset collected from other websites was used to evaluate the model's accuracy and performance.  

---

## 🔧 Technologies Used

- Python 🐍  
- FastAI library  
- Web scraping tools: BeautifulSoup, Requests  
- Pandas & NumPy for data analysis  
- Matplotlib & Seaborn for visualization  

---

## 🚀 Usage

1. Clone the repository:  
```bash
git clone https://github.com/gustavoroque97/heavy_metal_data.git
