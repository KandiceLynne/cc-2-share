import matplotlib.pyplot as plt

# Data for the two scenarios
unbalanced = [80, 20]
balanced = [50, 50]
labels = ['Work', 'Life']
colors = ['#ff9999','#66b3ff']  # professional, distinct colors

# Create subplots
fig, axes = plt.subplots(1, 2, figsize=(10, 5))

# Unbalanced pie chart
axes[0].pie(unbalanced, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors, textprops={'fontsize': 14})
axes[0].set_title('Unbalanced', fontsize=16)

# Balanced pie chart
axes[1].pie(balanced, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors, textprops={'fontsize': 14})
axes[1].set_title('Balanced', fontsize=16)

# Overall title and layout adjustments
plt.suptitle("Work-Life Balance:\nChoose wisely, don't give everything to unappreciative companies", fontsize=18)
plt.tight_layout(rect=[0, 0.03, 1, 0.9])
plt.show()
