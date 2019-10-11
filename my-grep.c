#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
		
	if (argv[1] == NULL) {
	printf("my-grep: searchterm [file ...]\n");
	exit(1);
	}
	
	char *term = argv[1];

	char *line = NULL;
	size_t len = 0;
	ssize_t read;
	FILE *stream;
		
	if (argv[2] == NULL) {
		stream = stdin;
		while ((read = getline(&line, &len, stream)) != -1) {
			if (strstr(line, term) != NULL){
				printf("%s", line);
			}
		}

		free(line);
		fclose(stream);
		exit(0);
	}
	else {
		int x = 2;
		while (argv[x] != NULL) {
			stream = fopen(argv[x], "r");
			
			if (stream == NULL) {
			printf("my-grep: cannot open file\n");
			exit(1);
			}
			while ((read = getline(&line, &len, stream)) != -1) {
				if (strstr(line, term) != NULL){
					printf("%s", line);
				}
			}

			free(line);
			fclose(stream);
			x++;
		}
	}
		
	return 0;
}
