import tkinter as tk
from tkinter import Scrollbar, Toplevel
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

spark = SparkSession.builder \
    .appName("app_2") \
    .master("spark://192.168.1.11:7077") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "6") \
    .getOrCreate()

NameTable = 'spark_add_data'

def insert_data():
    student_id = entry_student_id.get()
    mathematics = entry_mathematics.get()
    literature = entry_literature.get()
    foreign_language = entry_foreign_language.get()
    physics = entry_physics.get()
    chemistry = entry_chemistry.get()
    biology = entry_biology.get()
    history = entry_history.get()
    geography = entry_geography.get()
    civic_education = entry_civic_education.get()
    foreign_language_code = entry_foreign_language_code.get()
    
    try:
        mathematics = float(mathematics) if mathematics else None
        literature = float(literature) if literature else None
        foreign_language = float(foreign_language) if foreign_language else None
        physics = float(physics) if physics else None
        chemistry = float(chemistry) if chemistry else None
        biology = float(biology) if biology else None
        history = float(history) if history else None
        geography = float(geography) if geography else None
        civic_education = float(civic_education) if civic_education else None
    except ValueError:
        result_text.set("Điểm phải là một số.")
        return
    
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(NameTable)

    insert_query = """
        INSERT INTO students (
            student_id, 
            mathematics, 
            literature, 
            foreign_language, 
            physics, 
            chemistry, 
            biology, 
            history, 
            geography, 
            civic_education, 
            foreign_language_code
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    
    session.execute(insert_query, (
        student_id, 
        mathematics, 
        literature, 
        foreign_language, 
        physics, 
        chemistry, 
        biology, 
        history, 
        geography, 
        civic_education, 
        foreign_language_code
    ))
    
    result_text.set("Dữ liệu đã được chèn thành công.")
    update_data_listbox()


def edit_by_id_window():
    edit_window = tk.Toplevel(root)
    edit_window.title("Sửa Điểm Học Sinh Theo ID")

    label_edit_id = tk.Label(edit_window, text="Nhập ID học sinh:")
    entry_edit_id = tk.Entry(edit_window)
    label_edit_mathematics = tk.Label(edit_window, text="Mathematics:")
    entry_edit_mathematics = tk.Entry(edit_window)
    label_edit_literature = tk.Label(edit_window, text="Literature:")
    entry_edit_literature = tk.Entry(edit_window)
    label_edit_foreign_language = tk.Label(edit_window, text="Foreign Language:")
    entry_edit_foreign_language = tk.Entry(edit_window)
    label_edit_physics = tk.Label(edit_window, text="Physics:")
    entry_edit_physics = tk.Entry(edit_window)
    label_edit_chemistry = tk.Label(edit_window, text="Chemistry:")
    entry_edit_chemistry = tk.Entry(edit_window)
    label_edit_biology = tk.Label(edit_window, text="Biology:")
    entry_edit_biology = tk.Entry(edit_window)
    label_edit_history = tk.Label(edit_window, text="History:")
    entry_edit_history = tk.Entry(edit_window)
    label_edit_geography = tk.Label(edit_window, text="Geography:")
    entry_edit_geography = tk.Entry(edit_window)
    label_edit_civic_education = tk.Label(edit_window, text="Civic Education:")
    entry_edit_civic_education = tk.Entry(edit_window)
    label_edit_foreign_language_code = tk.Label(edit_window, text="Foreign Language Code:")
    entry_edit_foreign_language_code = tk.Entry(edit_window)
    button_edit_student = tk.Button(edit_window, text="Sửa", command=lambda: edit_student_by_id(
        entry_edit_id.get(),
        entry_edit_mathematics.get(),
        entry_edit_literature.get(),
        entry_edit_foreign_language.get(),
        entry_edit_physics.get(),
        entry_edit_chemistry.get(),
        entry_edit_biology.get(),
        entry_edit_history.get(),
        entry_edit_geography.get(),
        entry_edit_civic_education.get(),
        entry_edit_foreign_language_code.get(),
        edit_window
    ))
    label_edit_result = tk.Label(edit_window, text="")

    label_edit_id.grid(row=0, column=0, padx=10, pady=5, sticky="e")
    entry_edit_id.grid(row=0, column=1, padx=10, pady=5)
    label_edit_mathematics.grid(row=1, column=0, padx=10, pady=5, sticky="e")
    entry_edit_mathematics.grid(row=1, column=1, padx=10, pady=5)
    label_edit_literature.grid(row=2, column=0, padx=10, pady=5, sticky="e")
    entry_edit_literature.grid(row=2, column=1, padx=10, pady=5)
    label_edit_foreign_language.grid(row=3, column=0, padx=10, pady=5, sticky="e")
    entry_edit_foreign_language.grid(row=3, column=1, padx=10, pady=5)
    label_edit_physics.grid(row=4, column=0, padx=10, pady=5, sticky="e")
    entry_edit_physics.grid(row=4, column=1, padx=10, pady=5)
    label_edit_chemistry.grid(row=5, column=0, padx=10, pady=5, sticky="e")
    entry_edit_chemistry.grid(row=5, column=1, padx=10, pady=5)
    label_edit_biology.grid(row=6, column=0, padx=10, pady=5, sticky="e")
    entry_edit_biology.grid(row=6, column=1, padx=10, pady=5)
    label_edit_history.grid(row=7, column=0, padx=10, pady=5, sticky="e")
    entry_edit_history.grid(row=7, column=1, padx=10, pady=5)
    label_edit_geography.grid(row=8, column=0, padx=10, pady=5, sticky="e")
    entry_edit_geography.grid(row=8, column=1, padx=10, pady=5)
    label_edit_civic_education.grid(row=9, column=0, padx=10, pady=5, sticky="e")
    entry_edit_civic_education.grid(row=9, column=1, padx=10, pady=5)
    label_edit_foreign_language_code.grid(row=10, column=0, padx=10, pady=5, sticky="e")
    entry_edit_foreign_language_code.grid(row=10, column=1, padx=10, pady=5)
    button_edit_student.grid(row=11, column=0, columnspan=2, pady=10)
    label_edit_result.grid(row=12, columnspan=2, padx=10, pady=5)

def edit_student_by_id(student_id, mathematics, literature, foreign_language, physics, chemistry, biology, history, geography, civic_education, foreign_language_code, edit_window):
    try:
        mathematics = float(mathematics) if mathematics else None
        literature = float(literature) if literature else None
        foreign_language = float(foreign_language) if foreign_language else None
        physics = float(physics) if physics else None
        chemistry = float(chemistry) if chemistry else None
        biology = float(biology) if biology else None
        history = float(history) if history else None
        geography = float(geography) if geography else None
        civic_education = float(civic_education) if civic_education else None
    except ValueError:
        label_edit_result = tk.Label(edit_window, text="Điểm phải là một số.")
        label_edit_result.grid(row=12, columnspan=2, padx=10, pady=5)
        return

    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(NameTable)
    
    update_query = """
        UPDATE students
        SET mathematics = %s, 
            literature = %s, 
            foreign_language = %s, 
            physics = %s, 
            chemistry = %s, 
            biology = %s, 
            history = %s, 
            geography = %s, 
            civic_education = %s, 
            foreign_language_code = %s
        WHERE student_id = %s
    """
    
    session.execute(update_query, (
        mathematics, 
        literature, 
        foreign_language, 
        physics, 
        chemistry, 
        biology, 
        history, 
        geography, 
        civic_education, 
        foreign_language_code,
        student_id
    ))
    
    label_edit_result = tk.Label(edit_window, text="Dữ liệu đã được cập nhật thành công.")
    label_edit_result.grid(row=12, columnspan=2, padx=10, pady=5)

    update_data_listbox()

def delete_by_id_window():
    delete_window = tk.Toplevel(root)
    delete_window.title("Xóa Học Sinh Bằng ID")

    label_delete_id = tk.Label(delete_window, text="Nhập ID học sinh:")
    entry_delete_id = tk.Entry(delete_window)
    button_delete_id = tk.Button(delete_window, text="Xóa", command=lambda: delete_student_by_id(entry_delete_id.get(), delete_window))
    label_delete_result = tk.Label(delete_window, text="")

    label_delete_id.grid(row=0, column=0, padx=10, pady=5, sticky="e")
    entry_delete_id.grid(row=0, column=1, padx=10, pady=5)
    button_delete_id.grid(row=0, column=2, padx=10, pady=5)
    label_delete_result.grid(row=1, columnspan=3, padx=10, pady=5)

def delete_student_by_id(student_id, delete_window):
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(NameTable)
    
    delete_query = "DELETE FROM students WHERE student_id = %s"
    
    session.execute(delete_query, (student_id,))
    
    label_delete_result = tk.Label(delete_window, text="Dữ liệu đã được xóa thành công.")
    label_delete_result.grid(row=1, columnspan=3, padx=10, pady=5)

    update_data_listbox()


def search_by_id():
    search_window = tk.Toplevel(root)
    search_window.title("Tìm Kiếm Bằng ID")
    
    label_search_id = tk.Label(search_window, text="Nhập ID học sinh:")
    entry_search_id = tk.Entry(search_window)
    button_search = tk.Button(search_window, text="Tìm Kiếm", command=lambda: search_student(entry_search_id.get(), label_result))
    label_result = tk.Label(search_window, text="", wraplength=400)
    
    label_search_id.grid(row=0, column=0, padx=10, pady=5)
    entry_search_id.grid(row=0, column=1, padx=10, pady=5)
    button_search.grid(row=0, column=2, padx=10, pady=5)
    label_result.grid(row=1, column=0, columnspan=3, padx=10, pady=5)

def search_student(student_id, result_label):
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(NameTable)
    
    select_query = "SELECT * FROM students WHERE student_id = %s"
    row = session.execute(select_query, (student_id,)).one()
    
    if row:
        result = f"Student ID: {row.student_id}\nMathematics: {row.mathematics}\nLiterature: {row.literature}\nForeign Language: {row.foreign_language}\nPhysics: {row.physics}\nChemistry: {row.chemistry}\nBiology: {row.biology}\nHistory: {row.history}\nGeography: {row.geography}\nCivic Education: {row.civic_education}\nForeign Language Code: {row.foreign_language_code}"
    else:
        result = "Không tìm thấy học sinh có ID này."
    
    result_label.config(text=result)

def update_data_listbox():
    data_listbox.delete(0, tk.END)
    
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect(NameTable)
    
    rows = session.execute("SELECT * FROM students;")
    
    data_listbox.insert(tk.END, "Student ID - Mathematics - Literature - Foreign Language - Physics - Chemistry - Biology - History - Geography - Civic Education - Foreign Language Code")
    data_listbox.insert(tk.END, "-" * 130)
    
    for student_row in rows:
        data_listbox.insert(tk.END, f"{student_row.student_id} - {student_row.mathematics} - {student_row.literature} - {student_row.foreign_language} - {student_row.physics} - {student_row.chemistry} - {student_row.biology} - {student_row.history} - {student_row.geography} - {student_row.civic_education} - {student_row.foreign_language_code}")

root = tk.Tk()
root.title("Quản lý dữ liệu Cassandra")

label_student_id = tk.Label(root, text="Student ID:")
label_mathematics = tk.Label(root, text="Mathematics:")
label_literature = tk.Label(root, text="Literature:")
label_foreign_language = tk.Label(root, text="Foreign Language:")
label_physics = tk.Label(root, text="Physics:")
label_chemistry = tk.Label(root, text="Chemistry:")
label_biology = tk.Label(root, text="Biology:")
label_history = tk.Label(root, text="History:")
label_geography = tk.Label(root, text="Geography:")
label_civic_education = tk.Label(root, text="Civic Education:")
label_foreign_language_code = tk.Label(root, text="Foreign Language Code:")

entry_student_id = tk.Entry(root)
entry_mathematics = tk.Entry(root)
entry_literature = tk.Entry(root)
entry_foreign_language = tk.Entry(root)
entry_physics = tk.Entry(root)
entry_chemistry = tk.Entry(root)
entry_biology = tk.Entry(root)
entry_history = tk.Entry(root)
entry_geography = tk.Entry(root)
entry_civic_education = tk.Entry(root)
entry_foreign_language_code = tk.Entry(root)

button_insert = tk.Button(root, text="Thêm", command=insert_data)
button_edit_id = tk.Button(root, text="Sửa Theo ID", command=edit_by_id_window)
button_delete_id = tk.Button(root, text="Xóa Bằng ID", command=delete_by_id_window)
button_search_id = tk.Button(root, text="Tìm Kiếm Bằng ID", command=search_by_id)

result_text = tk.StringVar()
label_result = tk.Label(root, textvariable=result_text)

label_data = tk.Label(root, text="Dữ liệu hiện có:")
data_listbox_frame = tk.Frame(root)
data_listbox = tk.Listbox(data_listbox_frame, width=130)

scrollbar = Scrollbar(data_listbox_frame, orient="vertical", command=data_listbox.yview)
data_listbox.configure(yscrollcommand=scrollbar.set)

label_student_id.grid(row=0, column=0, padx=10, pady=5, sticky="e")
label_mathematics.grid(row=1, column=0, padx=10, pady=5, sticky="e")
label_literature.grid(row=2, column=0, padx=10, pady=5, sticky="e")
label_foreign_language.grid(row=3, column=0, padx=10, pady=5, sticky="e")
label_physics.grid(row=4, column=0, padx=10, pady=5, sticky="e")
label_chemistry.grid(row=5, column=0, padx=10, pady=5, sticky="e")
label_biology.grid(row=6, column=0, padx=10, pady=5, sticky="e")
label_history.grid(row=7, column=0, padx=10, pady=5, sticky="e")
label_geography.grid(row=8, column=0, padx=10, pady=5, sticky="e")
label_civic_education.grid(row=9, column=0, padx=10, pady=5, sticky="e")
label_foreign_language_code.grid(row=10, column=0, padx=10, pady=5, sticky="e")

entry_student_id.grid(row=0, column=1, padx=10, pady=5)
entry_mathematics.grid(row=1, column=1, padx=10, pady=5)
entry_literature.grid(row=2, column=1, padx=10, pady=5)
entry_foreign_language.grid(row=3, column=1, padx=10, pady=5)
entry_physics.grid(row=4, column=1, padx=10, pady=5)
entry_chemistry.grid(row=5, column=1, padx=10, pady=5)
entry_biology.grid(row=6, column=1, padx=10, pady=5)
entry_history.grid(row=7, column=1, padx=10, pady=5)
entry_geography.grid(row=8, column=1, padx=10, pady=5)
entry_civic_education.grid(row=9, column=1, padx=10, pady=5)
entry_foreign_language_code.grid(row=10, column=1, padx=10, pady=5)

button_insert.grid(row=11, column=0, padx=5, pady=10)
button_edit_id.grid(row=11, column=1, padx=5, pady=10)
button_delete_id.grid(row=11, column=2, padx=5, pady=10)
button_search_id.grid(row=12, column=0, columnspan=3, padx=5, pady=10)

label_result.grid(row=13, column=0, columnspan=3, padx=10, pady=5)
label_data.grid(row=14, columnspan=3, padx=10, pady=5)
data_listbox_frame.grid(row=15, columnspan=3, padx=10, pady=5)
data_listbox.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

update_data_listbox()

root.mainloop()

spark.stop()
