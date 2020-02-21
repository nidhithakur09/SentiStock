import tkinter as tk 

root= tk.Tk() 
   
canvas1 = tk.Canvas(root, width = 300, height = 300) 
canvas1.pack()
      
button1 = tk.Button (root, text='Exit Application', command=root.destroy) 
canvas1.create_window(150, 150, window=button1) 
    
root.mainloop()