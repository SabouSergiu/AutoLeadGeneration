





def main(path, name, column_names, column_website, n):    
    import subprocess
    import concurrent.futures
    import os
    # Delete the done.txt file if it exists
    programs_path =r"C:\Users\40757\Desktop\SergiuShortcut\scrappers\Google\GoogleSearchCompany"
    
    if os.path.exists(path+'\\done.txt'):
        os.remove(path+'\\done.txt')
        
    if os.path.exists(path+'\\done1.txt'):
        os.remove(path+'\\done1.txt')
    print(path, name, column_names, column_website, n)
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit the collect_data script to the executor
        collect_data = executor.submit(subprocess.run, ["python", programs_path+"\\start.py", path, name, column_names, str(n)])
        # Submit the find_owners script to the executor
        find_owners = executor.submit(subprocess.run, ["python", programs_path+"\\mid.py", path])
        # Submit the get_emails script to the executor
        get_emails = executor.submit(subprocess.run, ["python", programs_path+"\\finish.py", path, name, column_website,column_names])
        
        # p1 = executor.submit(start, path, name, column_names, n)
        # p2 = executor.submit(mid, path)
        # p3 = executor.submit(finish, path, name, column_website, column_names)
        
        # Wait for all the subprocesses to finish
        concurrent.futures.wait([collect_data, find_owners, get_emails])

    print("All scripts finished")

  


if __name__ == '__main__':
    

    # start_time = time.time()
    main(r"C:\Users\40757\Desktop\test", "data", "name","website", 1000)
    # end_time = time.time()

    # print(f"Execution time: {end_time - start_time} seconds")

#TO SOLVE:
#12. Linkedin might not be the one for the owner -> can be checked if linkedin scrape but only for those whose linkedin appear more than once with same name
#13. The position might not be the good one -> recheck with chatgpt (if good return 0 else return good_position) or improve prompt