import tkinter as tk
from tkinter import ttk


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Alexandros Ho Megas")

        # Text box
        self.text_box = tk.Text(root, height=5, width=100)
        self.text_box.pack(pady=10)

        # Table (Treeview)
        columns = ("Security Name", "Close", "Forecast", "Warscheinlichkeit", "Gewinn/Verlust","Strategie")
        self.tree = ttk.Treeview(root, columns=columns, show="headings")

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100)

        self.tree.pack(pady=10)

        # Add some sample data to the table
        self.populate_table()

        # Entry for adding new items
        self.new_item_label = tk.Label(root, text="New Item:")
        self.new_item_label.pack()

        self.new_item_entry = tk.Entry(root, width=30)
        self.new_item_entry.pack()

        # Button to add new item
        self.add_item_button = tk.Button(root, text="Add Item", command=self.add_item)
        self.add_item_button.pack(pady=10)

    def populate_table(self):
        # Add sample data to the table
        data = [
            ('us0970231058_ba', "100.23", "104.05","0.2797","3.82","ShortCall us0970231058_ba 103.98 - LongPut us58933y1055_mrk 87.11"),
            ('us58933y1055_mrk', "100.23", "104.05","0.6187","3.82","ShortCall us0970231058_ba 103.98 - LongPut us58933y1055_mrk 87.11"),
            ('us0258161092_axp', "100.23", "104.05","0.8891","3.82","ShortCall us0970231058_ba 103.98 - LongPut us58933y1055_mrk 87.11"),
        ]

        for row in data:
            self.tree.insert("", "end", values=row)

    def add_item(self):
        # Add a new item to the table
        new_item = self.new_item_entry.get()

        if new_item:
            # Get the next available ID
            current_ids = set(int(self.tree.item(item, "values")[0]) for item in self.tree.get_children())
            new_id = 1

            while new_id in current_ids:
                new_id += 1

            # Add the new item to the table
            self.tree.insert("", "end", values=(new_id, new_item, "New Description"))

            # Clear the entry
            self.new_item_entry.delete(0, tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
