# Python program to create a basic form 
# GUI application using the customtkinter module
import customtkinter as ctk
import tkinter as tk
import legoberry as lb

# Sets the appearance of the window
# Supported modes : Light, Dark, System
# "System" sets the appearance mode to 
# the appearance mode of the system
ctk.set_appearance_mode("Dark") 

# Sets the color of the widgets in the window
# Supported themes : green, dark-blue, blue 
ctk.set_default_color_theme("dark-blue") 

# Dimensions of the window
appWidth, appHeight = 650, 700


# App Class
class App(ctk.CTk):
  # The layout of the window will be written
  # in the init function itself
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

    # Sets the title of the window to "App"
    self.title("LegoBerry") 
    # Sets the dimensions of the window to 600x700
    self.geometry(f"{appWidth}x{appHeight}")   
    img = ctk.CTkImage("./images/logo.png")
    ctk.CTkButton(self, image = img).grid(row=0, column=0, columnspan=4, padx=20, pady=20, sticky="ew")
    # Name Label
    self.nameLabel = ctk.CTkLabel(self, text="Output File Name")
    self.nameLabel.grid(row=0, column=0, padx=20, pady=20, sticky="ew")
    # Name Entry Field
    self.nameEntry = ctk.CTkEntry(self, placeholder_text="generated_master.xlsx")
    self.nameEntry.grid(row=0, column=1, columnspan=2, padx=5, pady=5, sticky="ew")
    # Select Save Location Button
    self.selectSaveLocationButton = ctk.CTkButton(self, text="Select Location", command=self.select_save_location)
    self.selectSaveLocationButton.grid(row=0, column=3, columnspan=1, padx=10, pady=10, sticky="ew")

    # Max line size Label
    self.sizeLabel = ctk.CTkLabel(self, text="Max number of lines")
    self.sizeLabel.grid(row=1, column=0, padx=20, pady=20, sticky="ew")
    # Max line size Field
    self.sizeEntry = ctk.CTkEntry(self, placeholder_text="500")
    self.sizeEntry.grid(row=1, column=1, columnspan=3, padx=20, pady=20, sticky="ew")

    # School Type Label
    self.typeLabel = ctk.CTkLabel(self, text="School Type")
    self.typeLabel.grid(row=2, column=0, padx=20, pady=20,sticky="ew")
    # School Type Radio Buttons
    self.typeVar = tk.StringVar(value="HS")
    self.highRadioButton = ctk.CTkRadioButton(self, text="High School", variable=self.typeVar, value="HS")
    self.highRadioButton.grid(row=2, column=1, padx=20, pady=20, sticky="ew")
    self.middleRadioButton = ctk.CTkRadioButton(self, text="Middle School", variable=self.typeVar, value="She is")
    self.middleRadioButton.grid(row=2, column=2, padx=20, pady=20, sticky="ew")
    self.elementaryRadioButton = ctk.CTkRadioButton(self, text="Elementary School", variable=self.typeVar, value="ES")
    self.elementaryRadioButton.grid(row=2, column=3, padx=20, pady=20, sticky="ew")

    # # Choice Label
    # self.choiceLabel = ctk.CTkLabel(self, text="Choice")
    # self.choiceLabel.grid(row=3, column=0, padx=20, pady=20, sticky="ew")
    # # Choice Check boxes
    # self.checkboxVar = tk.StringVar(value="Choice 1")
    # self.choice1 = ctk.CTkCheckBox(self,text="choice 1", variable=self.checkboxVar, onvalue="choice1", offvalue="c1")
    # self.choice1.grid(row=3, column=1, padx=20, pady=20, sticky="ew")
    # self.choice2 = ctk.CTkCheckBox(self, text="choice 2", variable=self.checkboxVar, onvalue="choice2", offvalue="c2")               
    # self.choice2.grid(row=3, column=2, padx=20, pady=20, sticky="ew")

    # # Occupation Label
    # self.occupationLabel = ctk.CTkLabel(self, text="Occupation")
    # self.occupationLabel.grid(row=4, column=0, padx=20, pady=20, sticky="ew")

    # # Occupation combo box
    # self.occupationOptionMenu = ctk.CTkOptionMenu(self, values=["Student", "Working Professional"])
    # self.occupationOptionMenu.grid(row=4, column=1, padx=20, pady=20, columnspan=2, sticky="ew")

    # Generate Button
    self.generateResultsButton = ctk.CTkButton(self, text="Generate Results", command=self.generateResults)
    self.generateResultsButton.grid(row=5, column=1, columnspan=2, padx=20,  pady=20, sticky="ew")

    # Text Box
    self.displayBox = ctk.CTkTextbox(self, width=200, height=100)
    self.displayBox.grid(row=6, column=0, columnspan=4, padx=20, pady=20, sticky="nsew")

  def select_save_location(self):
    self.save_location = tk.filedialog.askopenfilename(initialdir = "/",
      title = "Select file",filetypes = (("jpeg files","*.jpg"),("all files","*.*")))
  
    # This function is used to insert the 
  # details entered by users into the textbox
  def generateResults(self):
    self.displayBox.delete("0.0", "200.0")
    text = self.createText()
    self.displayBox.insert("0.0", text)

  # This function is used to get the selected 
  # options and text from the available entry
  # fields and boxes and then generates 
  # a prompt using them
  def createText(self):
    checkboxValue = ""

    # .get() is used to get the value of the checkboxes and entryfields
    if self.typeVar.get() == "HS":
      self.geometry(f"{appWidth}x{appHeight}")
    # if self.choice1._check_state and self.choice2._check_state:
    #   checkboxValue += self.choice1.get() + " and " + self.choice2.get()
    # elif self.choice1._check_state:
    #   checkboxValue += self.choice1.get()
    # elif self.choice2._check_state:
    #   checkboxValue += self.choice2.get()
    # else:
    #   checkboxValue = "none of the available options"

    # # Constructing the text variable
    # text = f"{self.nameEntry.get()} : \n{self.typeVar.get()} {self.ageEntry.get()} years old and prefers {checkboxValue}\n"
    # text += f"{self.typeVar.get()} currently a {self.occupationOptionMenu.get()}"
    text = "Hello World"
    return text

if __name__ == "__main__":
  app = App()
  # Used to run the application
  app.mainloop()   
