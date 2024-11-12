def longestPalindrome_substring(s):
    s_length = len(s)

    #Identify the exact boundaries of the palindrome   
    def expand_from_center(left, right):
        while left >= 0 and right < s_length and s[left] == s[right]:
            left -= 1
            right += 1
        return left + 1, right - 1  
        
    start, end = 0, 0  
        
    for i in range(s_length):
        # Expand around both single character center (odd-length) and two-character center (even-length)
        for left, right in [(i, i), (i, i + 1)]:
            l, r = expand_from_center(left, right)
            if r - l > end - start:
                start, end = l, r
                    
    return s[start:end + 1]

#starting 
s=str(input("Enter Your string:"))
print("Longest Palindromic Substring is :" + longestPalindrome_substring(s))