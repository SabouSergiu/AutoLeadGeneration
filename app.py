import tkinter as tk
import pandas as pd
from tkinter import ttk, filedialog
import time
import threading
import os
import MULTI_single
import start_single, mid_single, finish_single
import MULTI
import threading

def parse_function(df):
    new_df = pd.DataFrame(columns=['name','rank','linkedin','email','company','website'])
    for i in range(len(df)):
        if(df.name[i]!="---"):
            for j in range(int(len(df.name[i].split("\n"))/2)):
                if(j>2):
                    break
                new_row = {'name': df.name[i].split("\n")[j*2],
                        'rank': df['rank'][i].split("\n")[j],
                        'linkedin': df.name[i].split("\n")[j*2+1],
                        'email': df['email'][i].split("\n")[j],
                        'company': df.company[i],
                        'website': df.website[i]}
                new_df = new_df.append(new_row, ignore_index=True)
        else:
            new_row = {'name': df.name[i],
                    'rank': df['rank'][i],
                    'linkedin': df.name[i],
                    'email': df['email'][i],
                    'company': df.company[i],
                    'website': df.website[i]}
            new_df = new_df.append(new_row, ignore_index=True)
    return new_df

def browse_file():
    file_path = filedialog.askopenfilename()
    print("Selected file:", file_path)
    path = os.path.dirname(file_path)
    filename = os.path.basename(file_path)
    input_boxes[0].config(state=tk.NORMAL)
    input_boxes[1].config(state=tk.NORMAL)
    print("Path:", path)
    print("Filename:", filename.replace('.csv', ''))
    input_boxes[0].delete(0, tk.END)  
    input_boxes[0].insert(0, path)  
    input_boxes[1].delete(0, tk.END)
    input_boxes[1].insert(0, filename.replace('.csv', ''))
    input_boxes[0].config(state=tk.DISABLED)
    input_boxes[1].config(state=tk.DISABLED)
    
    
def save_file():
    folder_path = filedialog.askdirectory()
    path = input_boxes[0].get()
    df = pd.read_csv(path+"\\finish.csv")
    df = MULTI_single.main_df(df)
    df = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email"]]
    df = df.rename(columns={"OwnerLinkedin": "linkedin"})
    df.to_csv(folder_path +"\\output.csv", index=False, encoding='utf-8-sig')


# Create a lock to synchronize program execution
lock = threading.Lock()

def run_program():

    input_boxes[0].config(state=tk.NORMAL)
    input_boxes[1].config(state=tk.NORMAL)
    path = input_boxes[0].get()
    name = input_boxes[1].get()
    column_names = input_boxes[2].get()
    column_website = input_boxes[3].get()
    n = input_boxes[4].get()
    input_boxes[0].config(state=tk.DISABLED)
    input_boxes[1].config(state=tk.DISABLED)
    input_boxes[2].config(state=tk.DISABLED)
    input_boxes[3].config(state=tk.DISABLED)
    input_boxes[4].config(state=tk.DISABLED)
    button.config(state=tk.DISABLED)
    stop_button.config(state=tk.ACTIVE)
    
    def run_in_background():
        MULTI.main(path, name, column_names, column_website, int(n))
        input_boxes[2].config(state=tk.NORMAL)
        input_boxes[3].config(state=tk.NORMAL)
        input_boxes[4].config(state=tk.NORMAL)
        button.config(state=tk.ACTIVE)
        stop_button.config(state=tk.DISABLED)
        save_button.config(state=tk.ACTIVE)
        

    # Create a new thread and start it
    thread = threading.Thread(target=run_in_background)
    thread.start()

#   6154
global signa 
signa = 0
def toggle_visibility():
    global signa
    if(signa == 0):
        thread = threading.Thread(target=read_csv_thread)
        signa = 1
        thread.start()
    if csv_frame.winfo_ismapped():
        csv_frame.grid_remove()
        csv_frame.place_forget()
    else:
        csv_frame.grid()
        csv_frame.place(x=25, y=325, width=950, height=250)
        

def find_single():
    company = input_boxes_single[0].get()
    website = input_boxes_single[1].get()
    input_boxes_single[0].config(state = tk.DISABLED)
    input_boxes_single[1].config(state = tk.DISABLED)
    output_single_info[0].delete(0, tk.END)
    output_single_info[1].delete(0, tk.END)
    output_single_info[2].delete(0, tk.END)
    output_single_info[3].delete(0, tk.END)
    output_single_info[0].config(state=tk.DISABLED)
    output_single_info[1].config(state=tk.DISABLED)
    output_single_info[2].config(state=tk.DISABLED)
    output_single_info[3].config(state=tk.DISABLED)
    button_single.config(state=tk.DISABLED)
    
    def run_in_background():
        df = start_single.main(company)
        df_owner = mid_single.main(df)
        
        if checkbox_var.get() == 1:  # Assuming checkbox_var is the variable associated with the checkbox
            df_em = finish_single.main(df_owner, website)
            name, rank, linkedin, email = MULTI_single.main(df_em)
            output_single_info[0].config(state=tk.NORMAL)
            output_single_info[0].insert(0, name.split('\n')[0])
            output_single_info[1].config(state=tk.NORMAL)
            output_single_info[1].insert(0, rank.split('\n')[0])
            output_single_info[2].config(state=tk.NORMAL)
            output_single_info[2].insert(0, linkedin.split('\n')[0])
            output_single_info[3].config(state=tk.NORMAL)
            output_single_info[3].insert(0, email.split('\n')[0])
        else:
            df_owner['email'] = '---'
            df_owner['website'] = website
            name, rank, linkedin, email = MULTI_single.main(df_owner)
            output_single_info[0].config(state=tk.NORMAL)
            output_single_info[0].insert(0, name.split('\n')[0])
            output_single_info[1].config(state=tk.NORMAL)
            output_single_info[1].insert(0, rank.split('\n')[0])
            output_single_info[2].config(state=tk.NORMAL)
            output_single_info[2].insert(0, linkedin.split('\n')[0])
            output_single_info[3].config(state=tk.NORMAL)
            output_single_info[3].insert(0, '---')
            
        input_boxes_single[0].config(state=tk.NORMAL)
        input_boxes_single[1].config(state=tk.NORMAL)
        button_single.config(state=tk.ACTIVE)
        

    # Create a new thread and start it
    thread = threading.Thread(target=run_in_background)
    thread.start()
global cos
cos = 0
def read_csv_thread():
    global cos
    path = input_boxes[0].get()
    while not window_closed:
        try:
            
            df = pd.read_csv(path+"\\finish.csv")
            
            # df = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email"]]
            if(cos == 0):
                df1 = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email"]]
                csv_label['columns'] = list(df1.columns)
                # print(list(df1.columns))
                # csv_label['columns'] = list(df.columns)
                csv_label['show'] = 'headings'
                
                for col in csv_label['columns']:
                    csv_label.heading(col, text=col)
                cos = 1

            #existing_rows = set([item for item in csv_label.get_children()])
            existing_rows = set([str(csv_label.item(child_id)['values'][0]) for child_id in csv_label.get_children()])
            
            df = MULTI_single.main_df(df)
            df1 = df.loc[:, ["company", "website", "name", "rank", "OwnerLinkedin", "email", "em_status"]]
            new_rows = df1.to_numpy().tolist()
            for row in new_rows:
                if row[0] not in existing_rows and str(row[6]) == '1':
                    csv_label.insert("", "end", values=row[:6])
                    existing_rows.add(row[0])
            time.sleep(5)
        except:
            time.sleep(1)
            pass


def stop_button_function():
    button.config(state=tk.ACTIVE)
    stop_button.config(state=tk.DISABLED)
    input_boxes[2].config(state=tk.NORMAL)
    input_boxes[3].config(state=tk.NORMAL)
    input_boxes[4].config(state=tk.NORMAL)
    
    

def on_closing():
    global window_closed
    window_closed = True
    window.destroy()


    

# Create the main window
window = tk.Tk()

window.title("Sergio's app")
window.geometry("1000x600")

window_closed = False
window.protocol("WM_DELETE_WINDOW", on_closing)

# Create a big title in the middle top of the window with underline
title_label = tk.Label(window, text="Sergiu's App", font=("Arial", 20, "bold", "underline"))
title_label.grid(row=0, column=0, columnspan=4, pady=20)

# Create the "Browse File" button
file_button = tk.Button(window, text="Browse File", command=browse_file)
file_button.grid(row=0, column=0, padx=10, pady=10)

save_button = tk.Button(window, text="SAVE", command=save_file)
save_button.grid(row=0, column=0, padx=10, pady=10)

# Create a frame to hold the entire framework
frame = tk.Frame(window, bd=2, relief="groove")
frame.grid(row=1, column=0, columnspan=3, padx=10, pady=10)

# Create input labels and input boxes
labels = ["Path", "fileName", "NameColumn", "WebColumn", "Number"]
input_boxes = []
input_boxes_single = []
df_headers = pd.read_csv(r"FILE PATH", nrows=0)


for i, label_text in enumerate(labels):
    label = tk.Label(frame, text=label_text)
    label.grid(row=i, column=0, padx=5, pady=10)

    input_box = tk.Entry(frame)
    input_box.grid(row=i, column=1, padx=5, pady=10)

    input_boxes.append(input_box)


# Create a button
button = tk.Button(window, text="Run", command=run_program, width=10, height=2)
button.grid(row=1, column=3, columnspan=1, pady=5)

# Create a button to toggle visibility of the frame
toggle_button = tk.Button(window, text="Show/Hide", command=toggle_visibility, width=10, height=2)
toggle_button.grid(row=1, column=4, columnspan=1, pady=5)

# Create a button to read CSV and display in the frame
stop_button = tk.Button(window, text="Stop", command=stop_button_function, width=10, height=2)
stop_button.grid(row=1, column=5, columnspan=1, pady=10)

# Create a frame to display CSV data
csv_frame = tk.Frame(window, bd=2, relief="groove", width = 500, height = 200)
csv_frame.grid(row=3, column=0, columnspan=5, padx=10, pady=10)

# Create a scrollbar
scrollbar = tk.Scrollbar(csv_frame)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

scrollbar_x = tk.Scrollbar(csv_frame, orient=tk.HORIZONTAL)
scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)

# Create a label inside the CSV frame to display CSV data
csv_label = ttk.Treeview(csv_frame, xscrollcommand=scrollbar_x.set, yscrollcommand=scrollbar.set, columns = df_headers.columns, show='headings')
csv_label.pack()

# Configure the scrollbar to scroll the csv_label
scrollbar.config(command=csv_label.yview)
scrollbar_x.config(command=csv_label.xview)


# Create a frame to hold the entire framework
frame_single = tk.Frame(window, bd=2, relief="groove")
frame_single.grid(row=1, column=0, columnspan=3, padx=10, pady=10)

labels_single = ['Company', 'Website']

for i, label_text in enumerate(labels_single):
    label_single = tk.Label(frame_single, text=label_text)
    label_single.grid(row=0, column=i*2, padx=5, pady=10)

    input_box_single = tk.Entry(frame_single)
    input_box_single.grid(row=0, column=i*2+1, padx=5, pady=10)

    input_boxes_single.append(input_box_single)

single_info = ['Name:','Position:','LinkedIn:','Email:']

frame_single_info = tk.Frame(window, bd=2, relief="groove")
frame_single_info.grid(row=1, column=0, columnspan=3, padx=10, pady=10)
output_single_info = []

for i, label_text in enumerate(single_info):
    input_label = tk.Label(frame_single_info, text=label_text, anchor="w")
    input_label.grid(row=i, column=0, padx=5, pady=10)

    input_value = tk.Entry(frame_single_info, width=45)
    input_value.grid(row=i, column=1, padx=5, pady=10)

    output_single_info.append(input_value)

# Create a button
button_single = tk.Button(window, text="run", command=find_single, width=10, height=2)
button_single.grid(row=1, column=3, columnspan=1, pady=5)

# Create a label for Bulk Search
bulk_search_label = tk.Label(window, text="Bulk Search", font=("Arial", 12))
bulk_search_label.grid(row=0, column=0, padx=10, pady=10)

# Create a label for Single Search
single_search_label = tk.Label(window, text="Single Search", font=("Arial", 12))
single_search_label.grid(row=0, column=3, padx=10, pady=10)


checkbox_var = tk.IntVar()
checkbox = tk.Checkbutton(window, text="", variable=checkbox_var)
checkbox.grid(row=1, column=6, padx=10, pady=10)



bulk_search_label.place(x=25, y=60)
single_search_label.place(x=490, y=60)
title_label.place(x=400, y=10)
button.place(x=360, y=130)
toggle_button.place(x=360, y=230)
stop_button.place(x=360, y=180)
frame.place(x=25, y=90)
csv_frame.place(x=25, y=325, width=950, height=250)
frame_single.place(x=490, y=90)
frame_single_info.place(x=490, y=133, width=398, height=165)
button_single.place(x=900, y=170)
checkbox.place(x=465, y=268, width=20, height=20,)
save_button.place(x=100, y=10)

stop_button.config(state=tk.DISABLED)
input_boxes[0].config(state=tk.DISABLED)
input_boxes[1].config(state=tk.DISABLED)
output_single_info[0].config(state=tk.DISABLED)
output_single_info[1].config(state=tk.DISABLED)
output_single_info[2].config(state=tk.DISABLED)
output_single_info[3].config(state=tk.DISABLED)
save_button.config(state=tk.DISABLED)
csv_frame.grid_remove()
csv_frame.place_forget()


window.mainloop()

#timelimit
#close threads with stop button