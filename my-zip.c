#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE (100)

int main(int argc, char *argv[]) {

		//char *stream;
	int x = 1;
	while (argv[x] != NULL) {
		FILE *fp = fopen(argv[x], "r");
		if (fp == NULL) {
		printf("my-zip: cannot open file\n");
		exit(1);
		}
		
		char buffer[BUFFER_SIZE];
		int count = 1;
		while (fgets(buffer, BUFFER_SIZE, fp) != NULL) {
			
			for (int i = 0; i<strlen(buffer); i++){
				
				if (buffer[i] == buffer[i+1]){
					count++;
				}
				else {
					fwrite(&count, sizeof(int), 1, stdout);
					fwrite(&(buffer[i]), sizeof(char), 1, stdout);
					count = 1;
				
			}
			

		}
		fclose(fp);
		x++;
	}
		return 0;
}
